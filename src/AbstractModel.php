<?php


namespace EasySwoole\ORM;

use ArrayAccess;
use EasySwoole\Mysqli\Client;
use EasySwoole\Mysqli\QueryBuilder;
use EasySwoole\ORM\Db\ClientInterface;
use EasySwoole\ORM\Db\Cursor;
use EasySwoole\ORM\Db\CursorInterface;
use EasySwoole\ORM\Db\Result;
use EasySwoole\ORM\Exception\Exception;
use EasySwoole\ORM\Relations\BelongsToMany;
use EasySwoole\ORM\Relations\HasMany;
use EasySwoole\ORM\Relations\HasOne;
use EasySwoole\ORM\Utility\FieldHandle;
use EasySwoole\ORM\Utility\PreProcess;
use EasySwoole\ORM\Utility\Schema\Table;
use EasySwoole\ORM\Utility\TableObjectGeneration;
use EasySwoole\ORM\Utility\TimeStampHandle;
use JsonSerializable;

/**
 * 抽象模型
 * Class AbstractMode
 * @package EasySwoole\ORM
 */
abstract class AbstractModel implements ArrayAccess, JsonSerializable
{
    /** @var Result */
    private $lastQueryResult;
    private $lastQuery;
    /* 快速支持连贯操作 */
    private $fields = "*";
    private $limit  = NULL;
    private $withTotalCount = FALSE;
    private $order  = NULL;
    private $where  = [];
    private $join   = NULL;
    private $group  = NULL;
    private $alias  = NULL;
    /** @var string 表名 */
    protected $tableName = '';
    /** @var Table */
    private static $schemaInfoList;
    /** @var string 连接池名称 */
    protected $connectionName = 'default';
    /** @var null|string 临时连接名 */
    private $tempConnectionName = null;
    /** @var array 当前的数据 */
    private $data = [];
    /** @var array 附加数据 */
    private $_joinData = [];
    /** @var array 未应用修改器和获取器之前的原始数据 */
    private $originData;
    /* 回调事件 */
    private $onQuery;
    /** @var string 临时表名 */
    private $tempTableName = null;
    /**@var ClientInterface */
    private $client;
    /** @var bool|string 是否开启时间戳 */
    protected  $autoTimeStamp = false;
    /** @var bool|string 创建时间字段名 false不设置 */
    protected  $createTime = 'create_time';
    /** @var bool|string 更新时间字段名 false不设置 */
    protected  $updateTime = 'update_time';
    /** @var array 预查询 */
    private $with;
    /** @var bool 是否为预查询 */
    private $preHandleWith = false;

    /**
     * AbstractModel constructor.
     * @param array $data
     * @throws Exception
     */
    public function __construct(array $data = [])
    {
        $this->data($data);
    }

    /**
     * 设置执行client
     * @param ClientInterface|null $client
     * @return $this
     */
    public function setExecClient(?ClientInterface $client)
    {
        $this->client = $client;
        return $this;
    }

    /**
     * 表结构信息
     * @param bool $isCache
     * @return Table
     * @throws Exception
     */
    public function schemaInfo(bool $isCache = true): Table
    {
        $key = md5(static::class);
        if (isset(self::$schemaInfoList[$key]) && self::$schemaInfoList[$key] instanceof Table && $isCache == true) {
            return self::$schemaInfoList[$key];
        }
        if ($this->tempConnectionName) {
            $connectionName = $this->tempConnectionName;
        } else {
            $connectionName = $this->connectionName;
        }
        if(empty($this->tableName)){
            throw new Exception("Table name is require for model ".static::class);
        }
        $tableObjectGeneration = new TableObjectGeneration(DbManager::getInstance()->getConnection($connectionName), $this->tableName);
        $schemaInfo = $tableObjectGeneration->generationTable();
        self::$schemaInfoList[$key] = $schemaInfo;
        return self::$schemaInfoList[$key];
    }


    /*  ==============    回调事件    ==================   */
    public function onQuery(callable $call)
    {
        $this->onQuery = $call;
        return $this;
    }

    /**
     * 调用事件
     * @param $eventName
     * @param array $param
     * @return bool|mixed
     */
    protected function callEvent($eventName, ...$param)
    {
        if(method_exists(static::class, $eventName)){
            return call_user_func([static::class, $eventName], $this, ...$param);
        }
        return true;
    }

    /*  ==============    快速支持连贯操作    ==================   */
    /**
     * @param mixed ...$args
     * @return AbstractModel
     */
    public function order(...$args)
    {
        $this->order[] = $args;
        return $this;
    }
    /**
     * @param int $one
     * @param int|null $two
     * @return $this
     */
    public function limit(int $one, ?int $two = null)
    {
        if ($two !== null) {
            $this->limit = [$one, $two];
        } else {
            $this->limit = $one;
        }
        return $this;
    }
    /**
     * @param $fields
     * @return $this
     */
    public function field($fields)
    {
        if (!is_array($fields)) {
            $fields = [$fields];
        }
        $this->fields = $fields;
        return $this;
    }
    /**
     * @return $this
     */
    public function withTotalCount()
    {
        $this->withTotalCount = true;
        return $this;
    }
    /**
     * @param $where
     * @return $this
     */
    public function where(...$where)
    {
        $this->where[] = $where;
        return $this;
    }
    /**
     * @param string $group
     * @return $this
     */
    public function group(string $group)
    {
        $this->group = $group;
        return $this;
    }
    /**
     * @param $joinTable
     * @param $joinCondition
     * @param string $joinType
     * @return $this
     */
    public function join($joinTable, $joinCondition, $joinType = '')
    {
        $this->join[] = [$joinTable, $joinCondition, $joinType];
        return $this;
    }

    /**
     * 别名设置
     * @param $alias
     * @return $this
     */
    public function alias($alias)
    {
        $this->alias = $alias;
        return $this;
    }

    /**
     * 预查询
     * @param $with
     * @return $this
     */
    public function with($with){
        if (is_string($with)){
            $this->with = explode(',', $with);
        } else if (is_array($with)){
            $this->with = $with;
        }
        return $this;
    }

    /**
     * 获取表名，如果有设置临时表名则返回临时表名
     * @throws
     */
    public function getTableName()
    {
        if($this->tempTableName !== null){
            return $this->tempTableName;
        }else{
           return $this->schemaInfo()->getTable();
        }
    }

    /**
     * 设置表名(一般用于分表)
     * @param string $name
     * @param bool $is_temp
     * @return $this
     * @throws Exception
     */
    public function tableName(string $name, bool $is_temp = false)
    {
        if ($is_temp){
            $this->tempTableName = $name;
        }else{
            if($name != $this->tableName){
                $this->tableName = $name;
                $this->schemaInfo(false);
            }
        }
        return $this;
    }

    private function parseTableName()
    {
        $table = $this->getTableName();
        if ($this->alias !== NULL){
            $table .= " AS `{$this->alias}`";
        }
        return $table;
    }

    /*  ==============    聚合查询    ==================   */

    /**
     * @param $field
     * @return null
     * @throws Exception
     * @throws \Throwable
     */
    public function max($field)
    {
        return $this->queryPolymerization('max', $field);
    }

    /**
     * @param $field
     * @return null
     * @throws Exception
     * @throws \Throwable
     */
    public function min($field)
    {
        return $this->queryPolymerization('min', $field);
    }

    /**
     * @param null $field
     * @return null
     * @throws Exception
     * @throws \Throwable
     */
    public function count($field = null)
    {
        return (int)$this->queryPolymerization('count', $field);
    }

    /**
     * @param $field
     * @return null
     * @throws Exception
     * @throws \Throwable
     */
    public function avg($field)
    {
        return $this->queryPolymerization('avg', $field);
    }

    /**
     * @param $field
     * @return null
     * @throws Exception
     * @throws \Throwable
     */
    public function sum($field)
    {
        return $this->queryPolymerization('sum', $field);
    }

    /*  ==============    Builder 和 Result    ==================   */
    public function lastQueryResult(): ?Result
    {
        return $this->lastQueryResult;
    }
    public function lastQuery(): ?QueryBuilder
    {
        return $this->lastQuery;
    }

    /**
     * 连接名设置
     * @param string $name
     * @param bool $isTemp
     * @return AbstractModel
     */
    function connection(string $name, bool $isTemp = false): AbstractModel
    {
        if ($isTemp) {
            $this->tempConnectionName = $name;
        } else {
            $this->connectionName = $name;
        }
        return $this;
    }

    /**
     * 获取器
     * @param $attrName
     * @return mixed|null
     */
    public function getAttr($attrName)
    {
        $method = 'get' . str_replace( ' ', '', ucwords( str_replace( ['-', '_'], ' ', $attrName ) ) ) . 'Attr';
        if (method_exists($this, $method)) {
            return $this->$method($this->data[$attrName] ?? null, $this->data);
        }
        // 判断是否有关联查询
        if (method_exists($this, $attrName)) {
            return $this->$attrName();
        }
        // 是否是附加字段
        if (isset($this->_joinData[$attrName])){
            return $this->_joinData[$attrName];
        }
        return $this->data[$attrName] ?? null;
    }

    /**
     * 设置器
     * @param $attrName
     * @param $attrValue
     * @param bool $setter
     * @return bool
     * @throws Exception
     */
    public function setAttr($attrName, $attrValue, $setter = true): bool
    {
        if (isset($this->schemaInfo()->getColumns()[$attrName])) {
            $col = $this->schemaInfo()->getColumns()[$attrName];
            $attrValue = PreProcess::dataValueFormat($attrValue, $col);
            $method = 'set' . str_replace( ' ', '', ucwords( str_replace( ['-', '_'], ' ', $attrName ) ) ) . 'Attr';
            if ($setter && method_exists($this, $method)) {
                $attrValue = $this->$method($attrValue, $this->data);
            }
            $this->data[$attrName] = $attrValue;
            return true;
        } else {
            $this->_joinData[$attrName] = $attrValue;
            return false;
        }
    }

    /**
     * @param null $where
     * @param bool $allow 是否允许没有主键删除
     * @return int|bool
     * @throws Exception
     * @throws \Throwable
     */
    public function destroy($where = null, $allow = false)
    {
        $builder = new QueryBuilder();
        $primaryKey = $this->schemaInfo()->getPkFiledName();

        if (is_null($where) && $allow == false) {
            if (empty($primaryKey)) {
                throw new Exception('Table not have primary key, so can\'t use Model::destroy($pk)');
            } else {
                $whereVal = $this->getAttr($primaryKey);
                if (empty($whereVal)) {
                    if (empty($this->where)){
                        throw new Exception('Table not have primary value');
                    }
                }else{
                    $builder->where($primaryKey, $whereVal);
                }
            }
        }

        PreProcess::mappingWhere($builder, $where, $this);
        $this->preHandleQueryBuilder($builder);
        $builder->delete($this->getTableName(), $this->limit);

        // beforeDelete事件
        $beforeRes = $this->callEvent('onBeforeDelete');
        if ($beforeRes === false){
            $this->callEvent('onAfterDelete', false);
            return false;
        }

        $this->query($builder);
        //  是否出错
        if ($this->lastQueryResult()->getResult() === false) {
            $this->callEvent('onAfterDelete', false);
            return false;
        }

        $this->callEvent('onAfterDelete', $this->lastQueryResult()->getAffectedRows());
        return $this->lastQueryResult()->getAffectedRows();
    }

    /**
     * 保存 插入
     * @throws Exception
     * @throws \Throwable
     * @return bool|int
     */
    public function save()
    {
        $builder = new QueryBuilder();
        $primaryKey = $this->schemaInfo()->getPkFiledName();
        if (empty($primaryKey)) {
            throw new Exception('save() needs primaryKey for model ' . static::class);
        }
        $rawArray = $this->toArray();
        // 合并时间戳字段
        $rawArray = TimeStampHandle::preHandleTimeStamp($this, $rawArray, 'insert');
        $builder->insert($this->getTableName(), $rawArray);
        $this->preHandleQueryBuilder($builder);
        // beforeInsert事件
        $beforeRes = $this->callEvent('onBeforeInsert');
        if ($beforeRes === false){
            $this->callEvent('onAfterInsert', false);
            return false;
        }

        $this->query($builder);
        if ($this->lastQueryResult()->getResult() === false) {
            $this->callEvent('onAfterInsert', false);
            return false;
        }

        $this->callEvent('onAfterInsert', true);
        if ($this->lastQueryResult()->getLastInsertId()) {
            $this->data[$primaryKey] = $this->lastQueryResult()->getLastInsertId();
            $this->originData = $this->data;
            return $this->lastQueryResult()->getLastInsertId();
        }
        return true;
    }

    /**
     * @param $data
     * @param bool $replace
     * @param bool $transaction 是否开启事务
     * @return array
     * @throws Exception
     * @throws \EasySwoole\Mysqli\Exception\Exception
     * @throws \Throwable
     */
    public function saveAll($data, $replace = true, $transaction = true)
    {
        $pk = $this->schemaInfo()->getPkFiledName();
        if (empty($pk)) {
            throw new Exception('saveAll() needs primaryKey for model ' . static::class);
        }

        if ($this->tempConnectionName) {
            $connectionName = $this->tempConnectionName;
        } else {
            $connectionName = $this->connectionName;
        }

        // 开启事务
        if ($transaction){
            DbManager::getInstance()->startTransaction($connectionName);
        }

        $result = [];
        try{
            foreach ($data as $key => $row){
                // 如果有设置更新
                if ($replace && isset($row[$pk])){
                    $model = static::create()->connection($connectionName)->get($row[$pk]);
                    unset($row[$pk]);
                    $model->update($row);
                    $result[$key] = $model;
                }else{
                    $model = static::create($row)->connection($connectionName);
                    $model->save();
                    $result[$key] = $model;
                }
            }
            if($transaction){
                DbManager::getInstance()->commit($connectionName);
            }
            return $result;
        } catch (\EasySwoole\Mysqli\Exception\Exception $e) {
            if($transaction) {
                DbManager::getInstance()->rollback($connectionName);
            }
            throw $e;
        } catch (\Throwable $e) {
            if($transaction) {
                DbManager::getInstance()->rollback($connectionName);
            }
            throw $e;
        }

    }

    /**
     * 获取数据
     * @param null $where
     * @return $this|null|array|bool
     * @throws Exception
     * @throws \EasySwoole\Mysqli\Exception\Exception
     * @throws \Throwable
     */
    public function get($where = null)
    {
        $builder = new QueryBuilder;
        $builder = PreProcess::mappingWhere($builder, $where, $this);
        $this->preHandleQueryBuilder($builder);
        $builder->getOne($this->parseTableName(), $this->fields);
        $res = $this->query($builder);

        if (empty($res)) {
            if ($res === false){
                return false;
            }
            return null;
        }
        
        if ($res instanceof CursorInterface){
            $res->setModelName(static::class);
            return $res;
        }

        $model = new static();

        $model->data($res[0], false);
        $model->lastQuery = $model->lastQuery();
        // 预查询
        if (!empty($model->with)){
            $model->preHandleWith($model);
        }
        return $model;
    }


    /**
     * 批量查询
     * @param null $where
     * @return array|bool|Cursor
     * @throws Exception
     * @throws \Throwable
     */
    public function all($where = null)
    {
        $builder = new QueryBuilder;
        $builder = PreProcess::mappingWhere($builder, $where, $this);
        $this->preHandleQueryBuilder($builder);
        $builder->get($this->parseTableName(), $this->limit, $this->fields);
        $results = $this->query($builder);
        $resultSet = [];
        if ($results === false){
            return false;
        }
        if ($results instanceof CursorInterface){
            $results->setModelName(static::class);
            return $results;
        }
        if (is_array($results)) {
            foreach ($results as $result) {
                $resultSet[] = (new static)->connection($this->connectionName)->data($result, false);
            }
            if (!empty($this->with)){
                $resultSet = $this->preHandleWith($resultSet);
            }
        }
        return $resultSet;
    }

    /**
     * @param string $column
     * @return array|null
     * @throws Exception
     * @throws \Throwable
     */
    public function column(?string $column = null): ?array
    {
        if (!is_null($column)) {
            $this->fields = [$column];
        }
        $this->all();

        return $this->lastQueryResult->getResultColumn($column);
    }

    /**
     * @param string $column
     * @return mixed
     * @throws Exception
     * @throws \Throwable
     */
    public function scalar(?string $column = null)
    {
        if (!is_null($column)) {
            $this->fields = [$column];
        }
        $this->limit = 1;
        $this->all();

        return $this->lastQueryResult->getResultScalar($column);
    }

    /**
     * @param string $column
     * @return array|null
     * @throws Exception
     * @throws \Throwable
     */
    public function indexBy(string $column): ?array
    {
        $this->all();

        return $this->lastQueryResult->getResultIndexBy($column);
    }

    /**
     * 直接返回某一行的某一列
     * @param $column
     * @return mixed|null
     * @throws Exception
     * @throws \EasySwoole\Mysqli\Exception\Exception
     * @throws \Throwable
     */
    public function val($column)
    {
        $data = $this->get();
        if (!$data) return $data;

        $data = $data->getAttr($column);
        if (!empty($data)) {
            return $data;
        } else {
            return NULL;
        }
    }

    /**
     * 更新
     * @param array $data
     * @param null $where
     * @param bool $allow 是否允许无条件更新
     * @return bool
     * @throws Exception
     * @throws \EasySwoole\Mysqli\Exception\Exception
     * @throws \Throwable
     */
    public function update(array $data = [], $where = null, $allow = false)
    {
        if (!empty($data)) {
            foreach ($data as $columnKey => $columnValue){
                $this->setAttr($columnKey, $columnValue);
            }
        }

        $attachData = [];
        // 遍历属性，把inc 和dec 的属性先处理
        foreach ($this->data as $tem_key => $tem_data){
            if (is_array($tem_data)){
                if (isset($tem_data["[I]"])){
                    $attachData[$tem_key] = $tem_data;
                    unset($this->data[$tem_key]);
                }
            }
        }

        $data = array_diff_assoc($this->data, $this->originData);
        $data = array_merge($data, $attachData);

        if (empty($data)){
            $this->originData = $this->data;
            return true;
        }

        $builder = new QueryBuilder();
        if ($where) {
            PreProcess::mappingWhere($builder, $where, $this);
        } else if (!$allow) {
            $pk = $this->schemaInfo()->getPkFiledName();
            if (isset($this->data[$pk])) {
                $pkVal = $this->data[$pk];
                $builder->where($pk, $pkVal);
            } else {
                if (empty($this->where)){
                    throw new Exception("update error,pkValue is require");
                }
            }
        }
        $this->preHandleQueryBuilder($builder);
        // 合并时间戳字段
        $data = TimeStampHandle::preHandleTimeStamp($this, $data, 'update');
        $builder->update($this->getTableName(), $data);

        // beforeUpdate事件
        $beforeRes = $this->callEvent('onBeforeUpdate');
        if ($beforeRes === false){
            $this->callEvent('onAfterUpdate', false);
            return false;
        }

        $results = $this->query($builder);
        if ($results){
            $this->originData = $this->data;
            $this->callEvent('onAfterUpdate', true);
        }else{
            $this->callEvent('onAfterUpdate', false);
        }

        return $results ? true : false;
    }


    // ================ 关联部分开始  ======================

    /**
     * 一对一关联
     * @param string        $class
     * @param callable|null $where
     * @param null          $pk
     * @param null          $joinPk
     * @param string        $joinType
     * @return mixed|null
     * @throws Exception
     * @throws \EasySwoole\Mysqli\Exception\Exception
     * @throws \ReflectionException
     * @throws \Throwable
     */
    protected function hasOne(string $class, callable $where = null, $pk = null, $joinPk = null, $joinType = '')
    {
        if ($this->preHandleWith === true){
            return [$class, $where, $pk, $joinPk, $joinType, 'hasOne'];
        }

        $fileName = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 2)[1]['function'];
        if (isset($this->_joinData[$fileName])) {
            return $this->_joinData[$fileName];
        }
        $result = (new HasOne($this, $class))->result($where, $pk, $joinPk, $joinType);
        $this->_joinData[$fileName] = $result;
        return $result;
    }

    /**
     * 一对多关联
     * @param string        $class
     * @param callable|null $where
     * @param null          $pk
     * @param null          $joinPk
     * @param string        $joinType
     * @return mixed|null
     * @throws
     */
    protected function hasMany(string $class, callable $where = null, $pk = null, $joinPk = null, $joinType = '')
    {
        if ($this->preHandleWith === true){
            return [$class, $where, $pk, $joinPk, $joinType, 'hasMany'];
        }
        $fileName = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 2)[1]['function'];
        if (isset($this->_joinData[$fileName])) {
            return $this->_joinData[$fileName];
        }
        $result = (new HasMany($this, $class))->result($where, $pk, $joinPk, $joinType);
        $this->_joinData[$fileName] = $result;
        return $result;
    }

    /**
     * 多对多关联
     * @param string $class
     * @param $middleTableName
     * @param null $pk
     * @param null $childPk
     * @return array|bool|Cursor|mixed|null
     * @throws Exception
     * @throws \ReflectionException
     * @throws \Throwable
     */
    protected function belongsToMany(string $class, $middleTableName, $pk = null, $childPk = null)
    {
        if ($this->preHandleWith === true){
            return [$class, $middleTableName, 'belongsToMany'];
        }
        $fileName = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 2)[1]['function'];
        if (isset($this->_joinData[$fileName])) {
            return $this->_joinData[$fileName];
        }
        $result = (new BelongsToMany($this, $class, $middleTableName, $pk, $childPk))->result();
        $this->_joinData[$fileName] = $result;
        return $result;
    }

    /**
     * 关联预查询
     * @param $data
     * @return mixed
     * @throws Exception
     * @throws \Throwable
     */
    private function preHandleWith($data)
    {
        // $data 只有一条 直接foreach调用 $data->$with();
        if ($data instanceof AbstractModel){// get查询使用
            foreach ($this->with as $with){
                $data->$with();
            }
            return $data;
        }else if (is_array($data) && !empty($data)){// all查询使用
            // $data 是多条，需要先提取主键数组，select 副表 where joinPk in (pk arrays);
            // foreach 判断主键，设置值
            foreach ($this->with as $with){
                $data[0]->preHandleWith = true;
                list($class, $where, $pk, $joinPk, $joinType, $withType) = $data[0]->$with();
                if ($pk !== null && $joinPk !== null){
                    $pks = array_map(function ($v) use ($pk){
                        return $v->$pk;
                    }, $data);
                    /** @var AbstractModel $insClass */
                    $insClass = new $class;
                    $insData  = $insClass->where($joinPk, $pks, 'IN')->all();
                    $temData  = [];
                    foreach ($insData as $insK => $insV){
                        if ($withType=='hasOne'){
                            $temData[$insV[$pk]] = $insV;
                        }else if($withType=='hasMany'){
                            $temData[$insV[$pk]][] = $insV;
                        }
                    }
                    foreach ($data as $model){
                        if (isset($temData[$model[$pk]])){
                            $model[$with] = $temData[$model[$pk]];
                        }
                    }
                    $data[0]->preHandleWith = false;
                } else {
                    // 闭包的只能一个一个调用
                    foreach ($data as $model){
                        foreach ($this->with as $with){
                            $model->$with();
                        }
                    }
                }
            }
            return $data;
        }
        return $data;
    }

    // ================ 关联部分结束  ======================

    // ================ Model内部底层支持开始  ======================

    /**
     * 实例化Model
     * @param array $data
     * @return AbstractModel|$this
     * @throws Exception
     */
    public static function create(array $data = []): AbstractModel
    {
        return new static($data);
    }

    /**
     * 数据赋值
     * @param array $data
     * @param bool $setter 是否调用setter
     * @return $this
     * @throws Exception
     */
    public function data(array $data, $setter = true)
    {
        foreach ($data as $key => $value) {
            $this->setAttr($key, $value, $setter);
        }
        $this->originData = $this->data;
        return $this;
    }

    /**
     * 类属性(连贯操作数据)清除
     */
    private function reset()
    {
        $this->tempConnectionName = null;
        $this->fields = "*";
        $this->limit  = null;
        $this->withTotalCount = false;
        $this->order  = null;
        $this->where  = [];
        $this->join   = null;
        $this->group  = null;
        $this->alias  = null;
        $this->tempTableName = null;
    }

    /**
     * 执行QueryBuilder
     * @param QueryBuilder $builder
     * @param bool $raw
     * @return mixed
     * @throws \Throwable
     */
    public function query(QueryBuilder $builder, bool $raw = false)
    {
        $start = microtime(true);
        $this->lastQuery = clone $builder;
        if ($this->tempConnectionName) {
            $connectionName = $this->tempConnectionName;
        } else {
            $connectionName = $this->connectionName;
        }
        try {
            $ret = null;
            if($this->client){
                $ret = DbManager::getInstance()->query($builder, $raw, $this->client);
            }else{
                $ret = DbManager::getInstance()->query($builder, $raw, $connectionName);
            }
            $builder->reset();
            $this->lastQueryResult = $ret;
            return $ret->getResult();
        } catch (\Throwable $throwable) {
            throw $throwable;
        } finally {
            $this->reset();
            if ($this->onQuery) {
                $temp = clone $builder;
                call_user_func($this->onQuery, $ret, $temp, $start);
            }
        }
    }

    /**
     * 连贯操作预处理
     * @param QueryBuilder $builder
     * @throws Exception
     * @throws \EasySwoole\Mysqli\Exception\Exception
     */
    public function preHandleQueryBuilder(QueryBuilder $builder)
    {
        // 快速连贯操作
        if ($this->withTotalCount) {
            $builder->withTotalCount();
        }
        if ($this->order && is_array($this->order)) {
            foreach ($this->order as $order){
                $builder->orderBy(...$order);
            }
        }
        if ($this->where) {
            $whereModel = new static();
            foreach ($this->where as $whereOne){
                if (is_array($whereOne[0]) || is_int($whereOne[0])){
                    $builder = PreProcess::mappingWhere($builder, $whereOne[0], $whereModel);
                }else{
                    $builder->where(...$whereOne);
                }
            }
        }
        if($this->group){
            $builder->groupBy($this->group);
        }
        if($this->join){
            foreach ($this->join as $joinOne) {
                $builder->join($joinOne[0], $joinOne[1], $joinOne[2]);
            }
        }
        // 如果在闭包里设置了属性，并且Model没设置，则覆盖Model里的
        if ( $this->fields == '*' ){
            $this->fields = implode(', ', $builder->getField());
        }

    }

    /**
     * 快捷查询 统一执行
     * @param $type
     * @param null $field
     * @return null|mixed
     * @throws Exception
     * @throws \Throwable
     */
    private function queryPolymerization($type, $field = null)
    {
        if ($field === null){
            $field = $this->schemaInfo()->getPkFiledName();
        }
        // 判断字段中是否带了表名，是否有`
        if (strstr($field, '`') == false){
            // 有表名
            if (strstr($field, '.') !== false){
                $temArray = explode(".", $field);
                $field = "`{$temArray[0]}`.`{$temArray[1]}`";
            }else{
                if(!is_numeric($field)){
                    $field = "`{$field}`";
                }
            }
        }

        $fields = "$type({$field})";
        $this->fields = $fields;
        $this->limit = 1;
        $res = $this->all();
        if (isset($res[0]->$fields)){
            return $res[0]->$fields;
        }

        return null;
    }

    /**
     * 取出链接
     * @param float|NULL $timeout
     * @return ClientInterface|null
     */
    public static function defer(float $timeout = null)
    {
        try {
            $model = new static();
        } catch (Exception $e) {
            return null;
        }
        $connectionName = $model->connectionName;

        return DbManager::getInstance()->getConnection($connectionName)->defer($timeout);
    }

    /**
     * 闭包注入QueryBuilder执行
     * @param callable $call
     * @return mixed
     * @throws \Throwable
     */
    function func(callable $call)
    {
        $builder = new QueryBuilder();
        $isRaw = (bool)call_user_func($call,$builder);
        return $this->query($builder,$isRaw);
    }

    /**
     * invoke执行Model
     * @param ClientInterface|Client $client
     * @param array $data
     * @return AbstractModel|$this
     * @throws Exception
     */
    public static function invoke(ClientInterface $client,array $data = []): AbstractModel
    {
        return (static::create($data))->setExecClient($client);
    }

    // ================ Model内部底层支持结束  ======================

    // ================ 魔术方法 JSON、取出、遍历等  ======================

    /**
     * ArrayAccess Exists
     * @param mixed $offset
     * @return bool
     */
    public function offsetExists($offset)
    {
        return isset($this->data[$offset]);
    }

    public function offsetGet($offset)
    {
        return $this->getAttr($offset);
    }

    /**
     * @param mixed $offset
     * @param mixed $value
     * @return bool
     * @throws Exception
     */
    public function offsetSet($offset, $value)
    {
        return $this->setAttr($offset, $value);
    }


    /**
     * @param mixed $offset
     * @return bool
     * @throws Exception
     */
    public function offsetUnset($offset)
    {
        return $this->setAttr($offset, null);
    }

    /**
     * json序列化方法
     * @return array|mixed
     */
    public function jsonSerialize()
    {
        $return = [];
        foreach ($this->data as $key => $data){
            if (method_exists($this, $key)){
                $return[$key] = $this->data[$key];
            }else{
                $return[$key] = $this->getAttr($key);
            }
        }
        foreach ($this->_joinData as $key => $data)
        {
            $return[$key] = $data;
        }
        return $return;
    }

    /**
     * Model数据转数组格式返回
     * @param bool $notNul
     * @param bool $strict
     * @return array
     */
    public function toArray($notNul = false, $strict = true): array
    {
        $temp = $this->data ?? [];
        if ($notNul) {
            foreach ($temp as $key => $value) {
                if ($value === null) {
                    unset($temp[$key]);
                }
            }
            if (!$strict) {
                $temp = $this->reToArray($temp);
            }
            return $temp;
        }
        if (is_array($this->fields)) {
            foreach ($temp as $key => $value) {
                if (in_array($key, $this->fields)) {
                    unset($temp[$key]);
                }
            }
        }else{
            if (!$strict) {
                $temp = $this->reToArray($temp);
            }
        }
        return $temp;
    }

    /**
     * 循环处理附加数据的toArray
     * @param $temp
     * @return mixed
     */
    private function reToArray($temp)
    {
        foreach ($this->_joinData as $joinField => $joinData){
            if (is_object($joinData) && method_exists($joinData, 'toArray')){
                $temp[$joinField] = $joinData->toArray();
            }else{
                $joinDataTem = $joinData;
                if(is_array($joinData)){
                    $joinDataTem = [];
                    foreach ($joinData as $key => $one){
                        if (is_object($one) && method_exists($one, 'toArray')){
                            $joinDataTem[$key] = $one->toArray();
                        }else{
                            $joinDataTem[$key] = $one;
                        }
                    }
                }
                $temp[$joinField] = $joinDataTem;
            }
        }
        return $temp;
    }

    public function __toString()
    {
        $data = array_merge($this->data, $this->_joinData ?? []);
        return json_encode($data, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    }

    /**
     * @param $name
     * @param $value
     * @throws Exception
     */
    function __set($name, $value)
    {
        $this->setAttr($name, $value);
    }

    function __get($name)
    {
        return $this->getAttr($name);
    }

    public function __isset($name)
    {
        if (isset($this->data[$name])) return true;

        // 是否是附加字段
        if (isset($this->_joinData[$name])) return true;

        $method = 'get' . str_replace( ' ', '', ucwords( str_replace( ['-', '_'], ' ', $name ) ) ) . 'Attr';
        if (method_exists($this, $method)) return true;

        // 判断是否有关联查询
        if (method_exists($this, $name)) return true;

        return false;
    }


    // ================ 以下为类属性和配置获取方法  ======================

    /**
     * 获取使用的链接池名
     * @return string|null
     */
    public function getConnectionName()
    {
        if ($this->tempConnectionName) {
            $connectionName = $this->tempConnectionName;
        } else {
            $connectionName = $this->connectionName;
        }
        return $connectionName;
    }

    /**
     * 获取自动更新时间戳设置
     * @return bool|string
     */
    public function getAutoTimeStamp()
    {
        return $this->autoTimeStamp;
    }

    /**
     * 获取创建时间的是否开启、字段名
     * @return bool|string
     */
    public function getCreateTime()
    {
        return $this->createTime;
    }

    /**
     *  获取更新时间的是否开启、字段名
     * @return bool|string
     */
    public function getUpdateTime()
    {
        return $this->updateTime;
    }

}
