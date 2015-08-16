<?php
/**
 * Created by IntelliJ IDEA.
 * User: ssingan
 * Date: 8/5/15
 * Time: 12:26 AM
 */

include("./SimpleDB.php");



$simpleDB = new SimpleDB();

$loopCondition  = true;

while($loopCondition) {

    $line = readline();
    $parts = preg_split('/\s+/', $line);


    if (count($parts) == 0 || count($parts) > 3) {
        print("Invalid input\n");
        break;
    }

    $command = strtoupper($parts[0]);

    switch($command) {
        case "END":
            $loopCondition = false;
            break;
        case "SET":
            if (count($parts) != 3) {
                print("Invalid input\n");
                $loopCondition = false;
                break;
            }

            $simpleDB->setValue($parts[1], $parts[2]);
            break;
        case "UNSET":
            if (count($parts) != 2) {
                print("Invalid input\n");
                $loopCondition = false;
                break;
            }

            $simpleDB->unsetKey($parts[1]);
            break;
        case "GET" :

            if (count($parts) != 2) {
                print("Invalid input\n");
                $loopCondition = false;
                break;
            }

            $value = $simpleDB->getValue($parts[1]);
            if ($value != null) {
                print($value . "\n");
            } else {
                print("NULL\n");
            }
            break;
        case "NUMEQUALTO" :

            if (count($parts) != 2) {
                print("Invalid input\n");
                $loopCondition = false;
                break;
            }

            print($simpleDB->getValueCount($parts[1]) . "\n");
            break;
        case "BEGIN" :
            $simpleDB->beginTransaction();
            break;
        case "ROLLBACK":
            if (!$simpleDB->rollBack()) {
                print("NO TRANSACTION\n");
            }
            break;
        case "COMMIT":
            if (!$simpleDB->commit()) {
                print("NO TRANSACTION\n");
            }
            break;
        default:
            $loopCondition = false;
            break;

    }



}

exit;
