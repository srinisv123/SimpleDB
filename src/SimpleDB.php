<?php
/**
 * Created by IntelliJ IDEA.
 *
 * Assumptions made:
 *
 * 1. All DB key/value pairs are strings
 *
 * Design:
 * 1. DB elements are stored as a hash map in the 'db' key in the first element of the dataDB structure
 * 2. value counts are stored on another hashmap in the 'values' key in the first element of the dataDB structure
 * 3. Transaction is just a new array in the dataDB structure
 * 4. Rollback involves popping the last dataDB
 * 5. Commit involves processing from the last transactions and applying it to the previous till we reach master
 *
 * User: ssingan
 * Date: 8/4/15
 * Time: 7:52 PM
 */

class SimpleDB {

    private $dataDB;
    const DB_KEY = 'db';
    const VALUES_KEY = 'values';
    const UNSET_LIST_KEY = 'unset';


    function __construct() {
        $this->dataDB = array($this->getDBStructure());
    }



    /**
     * Set a key/value pair in the DB
     * Time Complexity: O(1) Amortized
     * @param $key
     * @param $value
     */
    function setValue($key, $value) {

        // Fetch the current DB we are working on
        $currentDB = &$this->getCurrentDB();

        // If this key is already present, fetch the value so we can decrement the count
        $oldValue = $this->getValue($key);
        if ($oldValue != null) {

            // If the value is present, then decrement directly
            if (isset($currentDB[self::VALUES_KEY][$oldValue])) {
                $currentDB[self::VALUES_KEY][$oldValue]--;

                // cleanup the values as well
                $this->cleanUpEmptyValues($oldValue, $currentDB);

            } else { // In this case, record a -1, most likely in a transaction
                $currentDB[self::VALUES_KEY][$oldValue] = -1;
            }
        }

        // Set the new value in the current DB
        $currentDB[self::DB_KEY][$key] = $value;

        // If its in the unset list in current transaction, then remove it from that list
        if (isset($currentDB[self::UNSET_LIST_KEY][$key])) {
            unset($currentDB[self::UNSET_LIST_KEY][$key]);
        }

        // If the value already exists, increment its count.
        if (isset($currentDB[self::VALUES_KEY][$value])) {
            $currentDB[self::VALUES_KEY][$value]++;
        } else {
            $currentDB[self::VALUES_KEY][$value] =1;
        }
    }

    /**
     * Fetch a value from the DB
     * Time Complexity: O(t) Amortized where t is transaction depth
     * @param $key
     * @return null/string
     */
    function getValue($key) {
        $value = null;

        // Scan through all the DBs starting with master, and return the most recent value, if not found, returns null
        foreach ($this->dataDB as $currentDB) {
            if (isset($currentDB[self::DB_KEY][$key])) {
                $value =  $currentDB[self::DB_KEY][$key];
            }

            // If its in the unset list, reset value to null
            if (isset($currentDB[self::UNSET_LIST_KEY][$key])) {
                $value = null;
            }
        }

        return $value;
    }


    /**
     * Unset the key from the DB
     * Time Complexity: O(t) Amortized where t is transaction depth
     * @param $key
     */
    function unsetKey($key) {

        // Get the most recent value
        $value = $this->getValue($key);
        $currentDB = &$this->getCurrentDB();

        // If the value is not null
        if ($value != null) {

            // If the current transaction has this value set, then unset it
            if (isset($currentDB[self::DB_KEY][$key])) {
                unset($currentDB[self::DB_KEY][$key]);
            }

            // If the value is there in current transaction decrement it, else store a -1
            if (isset($currentDB[self::VALUES_KEY][$value])) {
                $currentDB[self::VALUES_KEY][$value]--;

                // cleanup the values as well
                $this->cleanUpEmptyValues($value, $currentDB);

            } else { // likely inside a transaction
                $currentDB[self::VALUES_KEY][$value] = -1;
            }

            // If we are not inside master, then update the unset list
            if (!$this->isMaster()) {
                $currentDB[self::UNSET_LIST_KEY][$key] = 1;
            }
        }

    }

    /**
     * Returns the count of the value
     * Time Complexity: O(t) Amortized where t is transaction depth
     * @param $value
     * @return int
     */
    function getValueCount($value) {

        $count = 0;
        foreach ($this->dataDB as $currentDB) {
            if (isset($currentDB[self::VALUES_KEY][$value])) {
                $count += $currentDB[self::VALUES_KEY][$value];
            }
        }

        return $count;
    }


    /**
     * Begin a new transaction
     */
    function beginTransaction() {
        $this->dataDB[] = $this->getDBStructure();
    }

    function rollBack() {
        if ($this->isMaster()) {
            return false;
        } else {
            array_pop($this->dataDB);
            return true;
        }
    }

    function commit() {
        if ($this->isMaster()) {
            return false;
        }



        while (count($this->dataDB)>1) {
            // Get the newest transaction block
            $committedTransaction = array_pop($this->dataDB);

            // Merge these elements into the previous transaction block
            $currentDB = &$this->getCurrentDB();

            // Commit the new keys
            foreach($committedTransaction[self::DB_KEY] as $dbKey => $dbValue) {
                $currentDB[self::DB_KEY][$dbKey] = $dbValue;
            }

            // commit the new values
            foreach($committedTransaction[self::VALUES_KEY] as $valueKey => $valueCount) {
                // If that value is present in current transaction update it
                if (isset($currentDB[self::VALUES_KEY][$valueKey])) {
                    $currentDB[self::VALUES_KEY][$valueKey] += $valueCount;

                    // cleanup the values as well
                    $this->cleanUpEmptyValues($valueKey, $currentDB);

                } else {
                    $currentDB[self::VALUES_KEY][$valueKey] = $valueCount;
                }
            }

            // apply the unset keys to remove the keys from the current DB, no need to update counts as that is already
            // taken care in the previous step and add it to the current unset list
            foreach($committedTransaction[self::UNSET_LIST_KEY] as $unsetKey => $discard) {
                if (isset($currentDB[self::DB_KEY][$unsetKey])) {
                    unset($currentDB[self::DB_KEY][$unsetKey]);
                }

                // If we are not the master, the carry forward the unset key list
                if (!$this->isMaster()) {
                    $currentDB[self::UNSET_LIST_KEY][$unsetKey] = $discard;
                }
            }
        }

        return true;
    }

    //////////////////////////////////////////////////////////////////////////////////
    // Helper functions
    //////////////////////////////////////////////////////////////////////////////////

    function &getCurrentDB() {
        return $this->dataDB[count($this->dataDB)-1];
    }

    function isMaster() {
        return count($this->dataDB) == 1 ? true : false;
    }

    function getDBStructure() {
        return array(
            self::DB_KEY => array(),
            self::UNSET_LIST_KEY => array(),
            self::VALUES_KEY => array(),
        );
    }

    function cleanUpEmptyValues($value, &$currentDB) {
        if ($currentDB[self::VALUES_KEY][$value] ==0) {
            unset($currentDB[self::VALUES_KEY][$value]);
        }
    }




}