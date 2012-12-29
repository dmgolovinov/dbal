<?php
/*
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * and is licensed under the MIT license. For more information, see
 * <http://www.phpdoctrine.org>.
 */

namespace Doctrine\DBAL\Schema;

use Doctrine\DBAL\Events;
use Doctrine\DBAL\Event\SchemaIndexDefinitionEventArgs;
use Doctrine\DBAL\Driver\SQLSrv\SQLSrvException;

/**
 * SQL Server Schema Manager
 *
 * @license     http://www.opensource.org/licenses/lgpl-license.php LGPL
 * @author      Konsta Vesterinen <kvesteri@cc.hut.fi>
 * @author      Lukas Smith <smith@pooteeweet.org> (PEAR MDB2 library)
 * @author      Juozas Kaziukenas <juozas@juokaz.com>
 * @since       2.0
 */
class SybaseSchemaManager extends AbstractSchemaManager
{
    /**
     * @override
     */
    protected function _getPortableTableColumnDefinition($tableColumn)
    {
        foreach ($tableColumn as $key => $value) {
            $tableColumn[strtoupper($key)] = $value;
        }
        
        $dbType = strtolower($tableColumn['TYPE_NAME']);
                             
        $autoincrement = false;
        if (stripos($dbType, 'identity')) {
            $dbType = trim(str_ireplace('identity', '', $dbType));
            $autoincrement = true;
        }

        $type = array();
        $unsigned = $fixed = null;

        if (!isset($tableColumn['name'])) {
            $tableColumn['name'] = '';
        }

        if (array_key_exists('COLUMN_DEF', $tableColumn)) {
            $default = $tableColumn['COLUMN_DEF'];
        }else{
            $default = 'NULL';
        }

        while ($default != ($default2 = preg_replace("/^\((.*)\)$/", '$1', $default))) {
            $default = trim($default2, "'");
        }

        $length = (int) $tableColumn['LENGTH'];

        $type = $this->_platform->getDoctrineTypeMapping($dbType);
        switch ($type) {
            case 'char':
                if ($tableColumn['LENGTH'] == '1') {
                    $type = 'boolean';
                    if (preg_match('/^(is|has)/', $tableColumn['name'])) {
                        $type = array_reverse($type);
                    }
                }
                $fixed = true;
                break;
            case 'text':
                $fixed = false;
                break;
        }
        switch ($dbType) {
            case 'nchar':
            case 'nvarchar':
            case 'ntext':
                // Unicode data requires 2 bytes per character
                $length = $length / 2;
                break;
        }

        $options = array(
            'length' => ($length == 0 || !in_array($type, array('text', 'string'))) ? null : $length,
            'unsigned' => (bool) $unsigned,
            'fixed' => (bool) $fixed,
            'default' => $default !== 'NULL' ? $default : null,
            'notnull' => (bool) (array_key_exists('IS_NULLABLE', $tableColumn) && $tableColumn['IS_NULLABLE'] != 'YES'),
            'scale' => $tableColumn['SCALE'],
            'precision' => $tableColumn['PRECISION'],
            'autoincrement' => $autoincrement,
        );

        return new Column($tableColumn['COLUMN_NAME'], \Doctrine\DBAL\Types\Type::getType($type), $options);
    }

    /**
     * @override
     */
    protected function _getPortableTableIndexesList($tableIndexRows, $tableName=null)
    {
        $indexBuffer = array();
        $stmt = $this->_conn->executeQuery( "EXEC sp_pkeys ('$tableName')" );
        $indexArray = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $indexName = $keys = "";
        foreach($indexArray as $indexColumnRow) {
            if(strlen($keys) == 0)
            {
                $indexName = $keys = $indexColumnRow['column_name'];
            }else{
                $keys += ', ' + $indexColumnRow['column_name'];
            }
        }
        if($keys !== "")
	{
		$stmt = $this->_conn->executeQuery( "EXEC sp_columns ('$tableName')" );
        	$indexArray = $stmt->fetchAll(\PDO::FETCH_ASSOC);
		foreach($indexArray as $indexColumnRow) {
            		if(strlen($keys) == 0)
            		{
		                $indexName = $keys = $indexColumnRow['column_name'];
		        }else{
		                $keys += ', ' + $indexColumnRow['column_name'];
		        }
		}

	}
	$data = array(
        	'name' => $indexName,
        	'columns' => explode(', ', $keys),
        	'unique' => true,
                'primary' => true,
        );
	$index = new Index('primary',$data['columns'], $data['unique'], $data['primary']);
	
	if ($index) {
            $indexBuffer['primary'] = $index;
        }
        
	$stmt = $this->_conn->executeQuery( "EXEC sp_statistics ('$tableName')" );
        $indexArray = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        foreach($indexArray as $indexColumnRow) {
            $indexName = $indexColumnRow['index_name'];
            $data = array(
                'name' => $indexName,
                'columns' => explode($indexColumnRow['column_name']),
                'unique' => strpos($indexColumnRow['non_unique'], '1') !== true,
                'primary' => false,
                );
            $index = new Index($data['name'], $data['columns'], $data['unique'], $data['primary']);
            if ($index) {
                $indexBuffer[$indexName] = $index;
            }

        }
        return $indexBuffer;
    }

    /**
     * @override
     */
    public function _getPortableTableForeignKeyDefinition($tableForeignKey)
    {
        return new ForeignKeyConstraint(
                (array) $tableForeignKey['pkcolumn_name'],
                $tableForeignKey['fktable_name'],
                (array) $tableForeignKey['fkcolumn_name'],
                "sybase_foreign_key_" . rand(),
                array(
                    'onUpdate' => str_replace('_', ' ', "NO_ACTION"),
                    'onDelete' => str_replace('_', ' ', "NO_ACTION"),
                )
        );
    }

    /**
     * @override
     */
    protected function _getPortableTableDefinition($table)
    {
        return $table['name'];
    }

    /**
     * @override
     */
    protected function _getPortableDatabaseDefinition($database)
    {
        return $database['name'];
    }

    /**
     * @override
     */
    protected function _getPortableViewDefinition($view)
    {
        // @todo
        return new View($view['name'], null);
    }

    /**
     * List the indexes for a given table returning an array of Index instances.
     *
     * Keys of the portable indexes list are all lower-cased.
     *
     * @param string $table The name of the table
     * @return Index[] $tableIndexes
     */
    public function listTableIndexes($table)
    {
        $sql = $this->_platform->getListTableIndexesSQL($table, $this->_conn->getDatabase());

        try {
            $tableIndexes = $this->_conn->fetchAll($sql);
        } catch(\PDOException $e) {
            if ($e->getCode() == "IMSSP") {
                return array();
            } else {
                throw $e;
            }
        } catch(SQLSrvException $e) {
            if (strpos($e->getMessage(), 'SQLSTATE [01000, 15472]') === 0) {
                return array();
            } else {
                throw $e;
            }
        }

        return $this->_getPortableTableIndexesList($tableIndexes, $table);
    }

    /**
     * @override
     */
    public function alterTable(TableDiff $tableDiff)
    {
        if(count($tableDiff->removedColumns) > 0) {
            foreach($tableDiff->removedColumns as $col){
                $columnConstraintSql = $this->getColumnConstraintSQL($tableDiff->name, $col->getName());
                foreach ($this->_conn->fetchAll($columnConstraintSql) as $constraint) {
                    $this->_conn->exec("ALTER TABLE $tableDiff->name DROP CONSTRAINT " . $constraint['Name']);
                }
            }
        }

        return parent::alterTable($tableDiff);
    }

    /**
     * This function retrieves the constraints for a given column.
     */
    private function getColumnConstraintSQL($table, $column)
    {
        return "SELECT SysObjects.[Name]
            FROM SysObjects INNER JOIN (SELECT [Name],[ID] FROM SysObjects WHERE XType = 'U') AS Tab
            ON Tab.[ID] = Sysobjects.[Parent_Obj]
            INNER JOIN sys.default_constraints DefCons ON DefCons.[object_id] = Sysobjects.[ID]
            INNER JOIN SysColumns Col ON Col.[ColID] = DefCons.[parent_column_id] AND Col.[ID] = Tab.[ID]
            WHERE Col.[Name] = " . $this->_conn->quote($column) ." AND Tab.[Name] = " . $this->_conn->quote($table) . "
            ORDER BY Col.[Name]";
    }
}
