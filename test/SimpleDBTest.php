<?php

include("../src/SimpleDB.php");
/**
 * Created by IntelliJ IDEA.
 * User: ssingan
 * Date: 8/4/15
 * Time: 7:57 PM
 */
class SimpleDBTest extends PHPUnit_Framework_TestCase {

    protected $simpleDB;
    function setUp() {

        $this->simpleDB = new SimpleDB();

    }


    function tearDown() {

    }

    function testSetValuesSimple() {
        $this->simpleDB->setValue("foo", "bar");
        $this->assertEquals("bar", $this->simpleDB->getValue("foo"), "Get the value that we inserted");
        $this->assertEquals(1, $this->simpleDB->getValueCount("bar"), "Check that the value count is 1");
    }

    function testSetValuesOverride() {
        // Insert simple value
        $this->simpleDB->setValue("foo", "bar");
        $this->assertEquals("bar", $this->simpleDB->getValue("foo"), "Get the value that we inserted");
        $this->assertEquals(1, $this->simpleDB->getValueCount("bar"), "Check that the value count is 1");

        // Override with a new value
        $this->simpleDB->setValue("foo", "bar1");
        $this->assertEquals("bar1", $this->simpleDB->getValue("foo"), "Get the latest value that we inserted");
        $this->assertEquals(1, $this->simpleDB->getValueCount("bar1"), "Check that the new value count is 1");
        $this->assertEquals(0, $this->simpleDB->getValueCount("bar"), "Check that the old value count is 0");


    }

    function testNonExistentValue() {
        $this->assertNull($this->simpleDB->getValue("foo"), "If data does not exist, return null");
        $this->assertEquals(0, $this->simpleDB->getValueCount("bar"), "Check that the value count is 0");

    }

    function testValueCount() {
        $this->assertEquals(0, $this->simpleDB->getValueCount("bar"), "Check that the value count is 0 initially");
        $this->simpleDB->setValue("foo", "bar");
        $this->assertEquals(1, $this->simpleDB->getValueCount("bar"), "Check that the value count is 1 after 1 set");
        $this->simpleDB->setValue("foo1", "bar");
        $this->assertEquals(2, $this->simpleDB->getValueCount("bar"), "Check that the value count is 2 after 2 sets");
    }

    function testUnsetKey() {
        $this->assertEquals(0, $this->simpleDB->getValueCount("bar"), "Check that the value count is 0 initially");
        $this->simpleDB->setValue("foo", "bar");
        $this->assertEquals(1, $this->simpleDB->getValueCount("bar"), "Check that the value count is 1 after 1 set");
        $this->simpleDB->unsetKey("foo");
        $this->assertEquals(0, $this->simpleDB->getValueCount("bar"), "Check that the value count down to 0 again");
        $this->assertNull($this->simpleDB->getValue("foo"), "Check that the get returns null");
    }

    function testSetValueOverrideWithTransactionAndRollback() {
        // Insert simple value
        $this->simpleDB->setValue("foo", "bar");
        $this->assertEquals("bar", $this->simpleDB->getValue("foo"), "Get the value that we inserted");
        $this->assertEquals(1, $this->simpleDB->getValueCount("bar"), "Check that the value count is 1");
        $this->assertTrue($this->simpleDB->isMaster(), "Check we are in master when there is no transaction");

        // Begin transaction
        $this->simpleDB->beginTransaction();
        $this->assertFalse($this->simpleDB->isMaster(), "Not in master anymore");

        // Insert same key with different value
        $this->simpleDB->setValue("foo", "bar1");
        $this->assertEquals("bar1", $this->simpleDB->getValue("foo"), "Get the value that we inserted most recently");
        $this->assertEquals(1, $this->simpleDB->getValueCount("bar1"), "Newly inserted value has 1 count");
        $this->assertEquals(0, $this->simpleDB->getValueCount("bar"), "Old value has 0 count");


        // Now rollback
        $this->assertTrue($this->simpleDB->rollBack(), "Rollback is successful");
        $this->assertTrue($this->simpleDB->isMaster(), "Check we are in master after 1 rollback");
        $this->assertEquals("bar", $this->simpleDB->getValue("foo"), "Get the old value after rollback");
        $this->assertEquals(0, $this->simpleDB->getValueCount("bar1"), "Rolled back value count is 0");
        $this->assertEquals(1, $this->simpleDB->getValueCount("bar"), "Old value has 1 count again");


    }

    function testSetValueOverrideWithSingleTransactionAndCommit() {

        // Insert simple value
        $this->simpleDB->setValue("foo", "bar");

        // Begin transaction
        $this->simpleDB->beginTransaction();

        // Insert same key with different value
        $this->simpleDB->setValue("foo", "bar1");

        // Now Commit
        $this->assertTrue($this->simpleDB->commit(), "Commit successful");
        $this->assertTrue($this->simpleDB->isMaster(), "Check we are in master after commit");
        $this->assertEquals("bar1", $this->simpleDB->getValue("foo"), "Get the most recent value");
        $this->assertEquals(1, $this->simpleDB->getValueCount("bar1"), "Most recent Value has count 1");
        $this->assertEquals(0, $this->simpleDB->getValueCount("bar"), "Old value has no count");


    }

    function testSetValueWithMultipleTransactionAndCommit() {

        // Insert simple value
        $this->simpleDB->setValue("foo", "bar");

        // Begin transaction
        $this->simpleDB->beginTransaction();

        // Insert same key with different value
        $this->simpleDB->setValue("foo1", "bar1");

        // Begin another transaction
        $this->simpleDB->beginTransaction();

        $this->assertEquals("bar", $this->simpleDB->getValue("foo"), "Get the most recent value");


        // Insert same key with different value
        $this->simpleDB->setValue("foo", "barMod");

        // Now Commit
        $this->assertTrue($this->simpleDB->commit(), "Commit successful");
        $this->assertTrue($this->simpleDB->isMaster(), "Check we are in master after commit");
        $this->assertEquals("barMod", $this->simpleDB->getValue("foo"), "Get the most recent value");
        $this->assertEquals(1, $this->simpleDB->getValueCount("barMod"), "Most recent Value has count 1");
        $this->assertEquals(0, $this->simpleDB->getValueCount("bar"), "Old value has no count");
        $this->assertEquals("bar1", $this->simpleDB->getValue("foo1"), "Get the most recent value of new key");
        $this->assertEquals(1, $this->simpleDB->getValueCount("bar1"), "Most recent Value has count 1");

        $expectedDBStructure = array (

                SimpleDB::DB_KEY => array (
                    "foo" => "barMod",
                    "foo1" => "bar1",
                ),
                SimpleDB::VALUES_KEY => array(
                    "bar1" => 1,
                    "barMod" =>1,
                ),
                SimpleDB::UNSET_LIST_KEY => array(

                )

        );

        $this->assertEquals($expectedDBStructure, $this->simpleDB->getCurrentDB(), "Check the structure of the current DB");



    }

    function testGetKeyValueFromAPreviousTransaction() {
        // Insert simple value
        $this->simpleDB->setValue("foo", "bar");

        // Begin 2 transactions
        $this->simpleDB->beginTransaction();
        $this->simpleDB->beginTransaction();


        $this->assertEquals("bar", $this->simpleDB->getValue("foo"), "Get the most recent value");


    }

    function testIncorrectCommitAndRollBack() {
        // Insert simple value
        $this->simpleDB->setValue("foo", "bar");

        $this->assertFalse($this->simpleDB->rollBack(), "Invalid rollback returns false");
        $this->assertFalse($this->simpleDB->commit(), "Invalid commit returns false");

    }

    function testUnsetWithTransactionAndRollBack() {

        // Insert simple value
        $this->simpleDB->setValue("foo", "bar");

        // begin transaction
        $this->simpleDB->beginTransaction();

        $this->simpleDB->unsetKey("foo");
        //echo(print_r($this->simpleDB,1));
        $this->assertEquals(null, $this->simpleDB->getValue("foo"), "There is no key");
        $this->assertEquals(0, $this->simpleDB->getValueCount("bar"), "There is no value count");

        // rollback
        $this->assertTrue($this->simpleDB->rollBack(), "rollback successful");

        $this->assertEquals("bar", $this->simpleDB->getValue("foo"), "Get back the old value");

        // rollback again should be false
        $this->assertFalse($this->simpleDB->rollBack(), "wrong rollback");

    }


    function testUnsetWithTransactionAndCommit() {

        // Insert simple value
        $this->simpleDB->setValue("foo", "bar");

        // begin transaction
        $this->simpleDB->beginTransaction();

        $this->simpleDB->unsetKey("foo");
        //echo(print_r($this->simpleDB,1));
        $this->assertEquals(null, $this->simpleDB->getValue("foo"), "There is no key");
        $this->assertEquals(0, $this->simpleDB->getValueCount("bar"), "There is no value count");

        // rollback
        $this->assertTrue($this->simpleDB->commit(), "commit successful");

        $this->assertEquals(null, $this->simpleDB->getValue("foo"), "Get back the old value");

        $expectedDBStructure = array (

            SimpleDB::DB_KEY => array (

            ),
            SimpleDB::VALUES_KEY => array(

            ),
            SimpleDB::UNSET_LIST_KEY => array(

            )

        );

        $this->assertEquals($expectedDBStructure, $this->simpleDB->getCurrentDB(), "Check the structure of the current DB");

        // rollback again should be false
        $this->assertFalse($this->simpleDB->rollBack(), "wrong rollback");

    }

    function testSetAndUnsetWithMultipleTransactionsWithRollBacksAndCommits() {

        // Insert simple value
        $this->simpleDB->setValue("foo", "bar");

        // begin transaction
        $this->simpleDB->beginTransaction();

        // Change the value
        $this->simpleDB->setValue("foo", "bar1");

        //being another transaction
        $this->simpleDB->beginTransaction();

        // unset the value
        $this->simpleDB->unsetKey("foo");
        $this->assertEquals(null, $this->simpleDB->getValue("foo"), "There is no key");
        $this->assertEquals(0, $this->simpleDB->getValueCount("bar"), "There is no value count");
        $this->assertEquals(0, $this->simpleDB->getValueCount("bar1"), "There is no value count");

        //echo(print_r($this->simpleDB,1));

        // rollback the last commit
        $this->assertTrue($this->simpleDB->rollBack(), "Rollback successful");

        $this->assertEquals("bar1", $this->simpleDB->getValue("foo"), "There is no key");
        $this->assertEquals(0, $this->simpleDB->getValueCount("bar"), "There is no value count");
        $this->assertEquals(1, $this->simpleDB->getValueCount("bar1"), "There is 1 value count");



        // add a new value
        $this->simpleDB->setValue("foo1", "bar1");
        $this->assertEquals(2, $this->simpleDB->getValueCount("bar1"), "There is 2 value count");

        // unset foo
        $this->simpleDB->unsetKey("foo");
        $this->assertEquals(null, $this->simpleDB->getValue("foo"), "There is no key");

        $this->assertTrue($this->simpleDB->commit(), "commit success");

        $this->assertEquals(null, $this->simpleDB->getValue("foo"), "There is no key");
        $this->assertEquals("bar1", $this->simpleDB->getValue("foo1"), "There is data");



        $expectedDBStructure = array (

            SimpleDB::DB_KEY => array (
                "foo1" => "bar1",
            ),
            SimpleDB::VALUES_KEY => array(
                "bar1" => 1,
            ),
            SimpleDB::UNSET_LIST_KEY => array(

            )

        );

        $this->assertEquals($expectedDBStructure, $this->simpleDB->getCurrentDB(), "Check the structure of the current DB");




    }

}
