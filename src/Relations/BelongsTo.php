<?php
/**
 * 一对多(逆向)
 * User: kwdd
 * Date: 2020/2/27
 * Time: 11:21
 */

namespace EasySwoole\ORM\Relations;


use EasySwoole\Mysqli\QueryBuilder;
use EasySwoole\ORM\AbstractModel;
use EasySwoole\ORM\Exception\Exception;
use EasySwoole\ORM\Utility\FieldHandle;
use EasySwoole\ORM\DbManager;

class BelongsTo
{
    private $fatherModel;
    private $childModel;

    public function __construct(AbstractModel $model, $class)
    {
        $this->childModel = $model;
        $this->fatherModel = $class;
    }

    /**
     * @param $where
     * @param $joinPk
     * @param $otherPk
     * @param $joinType
     * @return mixed
     * @throws \Throwable
     */
    public function result($where, $joinPk, $otherPk, $joinType)
    {
        $ref = new \ReflectionClass($this->fatherModel);

        if (!$ref->isSubclassOf(AbstractModel::class)) {
            throw new Exception("relation class must be subclass of AbstractModel");
        }

        /** @var AbstractModel $ins */
        $ins = $ref->newInstance();
        $builder = new QueryBuilder();
        $targetTable = $ins->schemaInfo()->getTable();
        $currentTable = $this->childModel->schemaInfo()->getTable();
        $pk = $this->childModel->schemaInfo()->getPkFiledName();

        if ($joinPk === null) {
            $dbName = DbManager::getInstance()->getConnection()->getConfig()->getDatabase();
            $queryBuilder = new QueryBuilder();
            $queryBuilder->raw("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE CONSTRAINT_SCHEMA='{$dbName}' AND TABLE_NAME='{$targetTable}' AND REFERENCED_TABLE_NAME='{$currentTable}' AND CONSTRAINT_NAME like 'fk_%';");
            $tableColumns = DbManager::getInstance()->query($queryBuilder, $raw = true, 'default');
            if (!empty($tableColumns->getResult())) {
                $joinPk = $tableColumns->getResult()[0]['COLUMN_NAME'];
            } else {
                return null;
            }
        }
        if ($otherPk === null) {
            $otherPk = $ins->schemaInfo()->getPkFiledName();
        }

        // 支持复杂的构造
        if ($where) {
            /** @var QueryBuilder $builder */
            $builder = call_user_func($where, $builder);
            $this->childModel->preHandleQueryBuilder($builder);
            $builder->get($currentTable, null, $builder->getField());
        } else {
            $targetTableAlias = "ES_INS";
            // 关联表字段自动别名
            $fields = FieldHandle::parserRelationFields($this->childModel, $ins, $targetTableAlias);

            $builder->join($targetTable." AS {$targetTableAlias}", "{$targetTableAlias}.{$otherPk} = {$currentTable}.{$joinPk}", $joinType)
                ->where("{$currentTable}.{$pk}", $this->childModel->$pk);
            $this->childModel->preHandleQueryBuilder($builder);
            $builder->get($currentTable, null, $fields);
        }

        $result = $this->childModel->query($builder);
        if ($result) {
            foreach ($result as $one) {
                // 分离结果 两个数组
                $targetData = [];
                $originData = [];
                foreach ($one as $key => $value){
                    if(isset($targetTableAlias)){
                        // 如果有包含附属别名，则是targetData
                        if (strpos($key, $targetTableAlias) !==  false){
                            $trueKey = substr($key,strpos($key,$targetTableAlias."_")+ strlen($targetTableAlias) + 1);
                            $targetData[$trueKey] = $value;
                        }else{
                            $originData[$key] = $value;
                        }
                    }else{
                        // callable $where 自行处理字段
                        $targetData[$key] = $value;
                    }
                }
                $return = ($ref->newInstance())->data($targetData);
            }

            return $return;
        }
        return null;
    }


    /**
     * @param array $data 原始数据 进入这里的处理都是多条 all查询结果
     * @param $withName string 预查询字段名
     * @param $where
     * @param $joinPk
     * @param $otherPk
     * @param $joinType
     * @return array
     * @throws Exception
     * @throws \Throwable
     */
    public function preHandleWith(array $data, $withName, $where, $joinPk, $otherPk, $joinType)
    {
        // 如果闭包不为空，则只能执行闭包
        if ($where !== null && is_callable($where)){
            // 闭包的只能一个一个调用
            foreach ($data as $model){
                foreach ($this->fatherModel->getWith() as $with){
                    $model->$with();
                }
            }
            return $data;
        }

        // 需要先提取主键数组，select 副表 where joinPk in (pk arrays);
        // foreach 判断主键，设置值
        $joinPks = array_map(function ($v) use ($joinPk){
            return $v->$joinPk;
        }, $data);
        $joinPks = array_values(array_unique($joinPks));

        /** @var AbstractModel $insClass */
        $insClass = new $this->childModel;
        $insData  = $insClass->where($otherPk, $joinPks, 'IN')->all();
        $temData  = [];
        foreach ($insData as $insK => $insV){
            $temData[$insV[$otherPk]][] = $insV;
        }
        foreach ($data as $model){
            $model[$withName] = [];
            if (isset($temData[$model[$joinPk]])){ // 如果在二维数组中，有属于A表模型主键的，那么就是它的子数据
                $model[$withName] = $temData[$model[$joinPk]];
            }
        }

        return $data;
    }
}