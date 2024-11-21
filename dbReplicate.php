<?php
/**
 * Simple script to sync databases.
 * It`s only copy missing rows starting from last and does not check for any updated data
 *
 * Before you start edit config file - dbReplicate.json
 */
declare(strict_types=1);

if (!extension_loaded('pdo') && !extension_loaded('pdo_mysql')) {
    echo 'PDO driver is not installed';
}

/**
 * Get list of DB tables
 *
 * @param PDO $pdoConnection
 * @return array
 */
function listDbTables(PDO $pdoConnection): array
{
    $query = $pdoConnection->query('SHOW TABLES');
    return $query->fetchAll(PDO::FETCH_COLUMN);
}

function listTableFields(PDO $pdoConnection, string $tableName): array
{
    $query = $pdoConnection->query("DESCRIBE $tableName");
    return $query->fetchAll(PDO::FETCH_ASSOC);
}

const FONT_COLOR_BLUE = 'blue';
const FONT_COLOR_CYAN = 'cyan';
const FONT_COLOR_GREEN   = 'green';
const FONT_COLOR_PURPLE = 'purple';
const FONT_COLOR_RED = 'red';
const FONT_COLOR_WHITE = 'white';
const FONT_COLOR_YELLOW = 'yellow';

$COLORS = [
    FONT_COLOR_RED => '0;31',
    FONT_COLOR_GREEN => '0;32',
    FONT_COLOR_YELLOW => '1;33',
    FONT_COLOR_BLUE => '0;34',
    FONT_COLOR_PURPLE => '0;35',
    FONT_COLOR_CYAN => '0;36',
    FONT_COLOR_WHITE => '1;37'
];

/**
 * Display message on screen
 *
 * @param string $message
 * @param string $color
 * @return void
 */
function showMessage(string $message, string $color = FONT_COLOR_WHITE): void
{
    global $COLORS;
    $colorCode = $COLORS[$color] ?? '0';
    echo "\033[" . $colorCode . "m" . $message . "\033[0m\n";
}

/**
 * @param PDO $masterConnection
 * @param mixed $tableName
 * @param PDO $slaveConnection
 * @param mixed $slaveRowsCount
 * @return void
 */
function syncTableRows(PDO $masterConnection, PDO $slaveConnection, string $tableName, int $slaveRowsCount): void
{
    $masterFields = listTableFields($masterConnection, $tableName);
    $slaveFields = listTableFields($slaveConnection, $tableName);
    if (count($masterFields) !== count($slaveFields)) {
        showMessage("Table '$tableName' has different number of columns.", FONT_COLOR_RED);
        die;
    }

    $implodeFields = implode(', ', (array_column($masterFields, 'Field')));
    global $config;
    $getMissingDataQuery = "SELECT $implodeFields FROM $tableName LIMIT {$config['maxRowsToSyncFromOneTable']} OFFSET $slaveRowsCount";
    $stmt = $masterConnection->prepare($getMissingDataQuery);
    $stmt->execute();
    $rows = $stmt->fetchAll();
    $rowsInserted = 0;
    foreach ($rows as $row) {
        try {
            // Prepare an SQL statement for execution
            foreach ($row as $key => $value) {
                $row[":$key"] = $value;
                unset($row[$key]);
            }
            $prepareFieldsString = implode(', ', array_keys($row));
            $stmt = $slaveConnection->prepare("INSERT INTO $tableName ($implodeFields) VALUES ($prepareFieldsString)");
            $stmt->execute($row);
            $rowsInserted++;
        } catch (\PDOException $e) {
            showMessage("Error: {$e->getMessage()}", FONT_COLOR_RED);
        }
    }
    showMessage("Total rows inserted '$rowsInserted' into '$tableName'");
}

$dbConnectionOptions = [
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    PDO::ATTR_EMULATE_PREPARES => false,
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_TIMEOUT => 5,
];

///
/// Logic goes next
///
$configJson = file_get_contents('dbReplicate.json');
$config = json_decode($configJson, true);

try {
    $masterConnection = new PDO("{$config['masterDbConfig']['driver']}:host={$config['masterDbConfig']['host']};port={$config['masterDbConfig']['port']};dbname={$config['masterDbConfig']['dbName']};charset={$config['masterDbConfig']['charset']}", $config['masterDbConfig']['userName'], $config['masterDbConfig']['password'], $dbConnectionOptions);
    $slaveConnection = new PDO("{$config['slaveDbConfig']['driver']}:host={$config['slaveDbConfig']['host']};port={$config['slaveDbConfig']['port']};dbname={$config['slaveDbConfig']['dbName']};charset={$config['slaveDbConfig']['charset']}", $config['slaveDbConfig']['userName'], $config['slaveDbConfig']['password'], $dbConnectionOptions);
} catch (PDOException $e) {
    showMessage("Connection failed: {$e->getMessage()}", FONT_COLOR_PURPLE);
    die;
}

$masterDbTables = listDbTables($masterConnection);
$slaveDbTables = listDbTables($slaveConnection);

$diffTables = array_diff($masterDbTables, $slaveDbTables);
if ($diffTables) {
    die('Tables are missing in slave DB: ' . implode(', ', $diffTables));
}

foreach ($masterDbTables as $tableName) {
    $query = "SELECT COUNT(*) FROM $tableName";
    $stmtM = $masterConnection->query($query);
    $masterRowsCount = $stmtM->fetchColumn();
    $stmtS = $slaveConnection->query($query);
    $slaveRowsCount = $stmtS->fetchColumn();
    if ($masterRowsCount == $slaveRowsCount) {
        showMessage("Number of rows for table '$tableName' are identical", FONT_COLOR_GREEN);
        continue;
    }
    // sync table rows
    syncTableRows($masterConnection, $slaveConnection, $tableName,  $slaveRowsCount);
}

showMessage('FINISHED !!!', FONT_COLOR_GREEN);
