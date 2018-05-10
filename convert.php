<?php

error_reporting(E_ALL & ~E_NOTICE);

foreach (array('pdo','pdo_pgsql','pdo_mysql') as $ext) {
    if (!extension_loaded($ext))
        exit("extension {$ext} not supported!");
}

$mysql_dsn = 'mysql://username:password@server_ip:port/databasename';
$pgsql_dsn = 'pgsql://username:password@server_ip:port/databasename';

$mysql = database::get_instance($mysql_dsn);
$pgsql = database::get_instance($pgsql_dsn);

$mysql_version = $mysql->prepare('select version()')->fetchColumn();
$pgsql_version = $pgsql->prepare("show server_version")->fetchColumn();

line();
printl("MySQL Version:{$mysql_version}\nPostgreSQL Version:{$pgsql_version}");
line();

// 获取MySQL所有表
$mysql_tables = $mysql->prepare('show tables')->fetchAll(3);

if (empty($mysql_tables))
    printl("MySQL tables empty.") && exit;

sleep(2);

printl(null);
printl(null);
printl('Tables:');
sleep(1);
line();

try {
    $pgsql->beginTransaction();
    foreach ($mysql_tables as $_table) {
        $_t = $_table[0];
        printl($_t);

        // get tables
        $table_desc = $mysql->prepare('desc '.$_t)->fetchAll(2);
        // create tables
        echo 'create table...';
        $sql = 'CREATE TABLE "public"."'.$_t.'" (';
        foreach ($table_desc as $_tdesc) {
            $sql .= mysql_table_desc_convert($_tdesc);
        }
        $sql = substr($sql,0,-1);
        $sql .= ') WITH (OIDS=FALSE)';
        $pgsql->prepare($sql);
        printl('success');
        usleep(99000);

        // create indexes
        echo 'checking indexes...';
        $indexes = $mysql->prepare('show index from '.$_t)->fetchAll(2);

        $_pri = $_uni = $_ind = array();
        foreach ($indexes as $_index) {
            if ($_index['Key_name'] == 'PRIMARY')
                $_pri[] = $_index['Column_name'];
            else if ($_index['Non_unique'] == 0)
                $_uni[$_index['Key_name']][] = $_index['Column_name'];
            else
                $_ind[$_index['Key_name']][] = $_index['Column_name'];
        }
        // create primary
        if (!empty($_pri)) {
            $sql = "ALTER TABLE \"public\".\"{$_t}\" ADD PRIMARY KEY (".'"'.implode('","',$_pri).'"'.")";
            $pgsql->prepare($sql);
        }
        // create unique index
        foreach ($_uni as $name=>$columns) {
            $sql = "CREATE UNIQUE INDEX \"{$_t}_{$name}\" ON \"public\".\"{$_t}\" USING btree(".'"'.implode('","',$columns).'"'.")";
            $pgsql->prepare($sql);
        }
        // create normal index
        foreach ($_ind as $name=>$columns) {
            $sql = "CREATE  INDEX \"{$_t}_{$name}\" ON \"public\".\"{$_t}\" USING btree(".'"'.implode('","',$columns).'"'.")";
            $pgsql->prepare($sql);
        }
        printl('success');

        // transfer data
        echo 'transfering data...';
        $data = $mysql->prepare('select * from '.$_t)->fetchAll(2);
        while ($row = $mysql->prepare('select * from '.$_t)->fetch(2)) {
            $sql = '(';
            foreach ($row as $key=>$val) {
                $sql .= (is_numeric($val) ? $val : "'{$val}'").',';
            }
            $sql = substr($sql,0,-1);
            $sql .= '),';
            $sql = "INSERT INTO {$_t} VALUES ".substr($sql,0,-1);
            $pgsql->prepare($sql);
        }
        printl('success');
    }
    $pgsql->commit();
} catch(PDOException $e) {
    $pgsql->rollback();
    printl('faield');
    line();
    printl('SQL');
    printl(iconv('utf-8','gbk',$sql));
    line();
    printl('Message');
    printl($e->getMessage());
    exit;
}

class database {

    private $db = null;

    static private $instance = array();

    private function __construct() {
        //
    }

    static public function get_instance($dsn) {
        $hash = md5($dsn);
        if (empty(self::$instance[$hash])) {
            self::$instance[$hash] = new self();
            self::$instance[$hash]->connect($dsn);
        }

        return self::$instance[$hash];
    }

    private function connect($dsn) {
        try {
            $dsn_result = $this->parseDsn($dsn);
            extract($dsn_result);
            $this->db = new PDO($dsn, $username, $password,array(
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
                ));
            $this->db->exec("set names 'utf8'");
        } catch (PDOException $e) {
            exit($e->getMessage());
        }
    }

    public function prepare() {
        $args = func_get_args();

        $sql = array_shift($args);

        if (!$sql)
            return false;

        $stmt = $this->db->prepare($sql);

        if (!empty($args[0]) && is_array($args[0]))
            $args = $args[0];

        $stmt->execute($args);

        return $stmt;
    }

    public function beginTransaction() {
        return $this->db->beginTransaction();
    }
    public function commit() {
        return $this->db->commit();
    }
    public function rollback() {
        return $this->db->rollback();
    }


    private function __clone() {
        //
    }

    private function parseDsn($dsn) {
        $uri = parse_url($dsn);

        $dsn = sprintf('%s:host=%s;dbname=%s', $uri['scheme'], $uri['host'], substr($uri['path'],1));

        if (!empty($uri['port']))
            $dsn .= ";port=".$uri['port'];

        return array(
                'dsn' => $dsn,
                'username' => $uri['user'],
                'password' => $uri['pass']
            );
    }
}


function printl($msg) {
    echo "$msg\n";
}
function line() {
    printl(str_repeat('=',50));
}

function mysql_table_desc_convert(Array $tdesc) {
    $from_type_str = str_replace(' unsigned', NULL,$tdesc['Type']);
    $not_null = $tdesc['Null'] == 'NO' ? 'NOT NULL' : NULL;

    if (in_array($tdesc['Field'],array('ctid','cmax','xmax','xmin','tableoid','oid'))) {
        printl(NULL);
        line();
        printl('system column: '.$tdesc['Field']);
        line();
        sleep(2);
        $tdesc['Field'] = 'p'.$tdesc['Field'];
    }

    if ($tdesc['Extra'])
        $type = 'bigserial';
    else if ('enum' === substr(strtolower($tdesc['Type']),0,4)) {
        $type = str_replace('enum','character varying check('.$tdesc['Field'].' in ', $from_type_str).')';
    }
    else {
        $from_type = substr($from_type_str,0,($length = stripos($from_type_str,'('))!==false ? $length : strlen($from_type_str)-1);

        $type = str_replace(
            array('varchar','small','int','big','tiny','medium','blob','enum','float'),
            array('character varying','','numeric','','','','bytea','character varying','numeric'),
            $from_type_str);
    }

    $default = $tdesc['Default'] ? 'DEFAULT '.(is_numeric($tdesc['Default']) ? $tdesc['Default'] : "'{$tdesc['Default']}'") : NULL;

    return ' "'.$tdesc['Field'].'"  '.$type.' '.$default.' '.$not_null." ,";
}
