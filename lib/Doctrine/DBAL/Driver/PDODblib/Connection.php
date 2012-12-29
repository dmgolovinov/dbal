<?php
namespace Doctrine\DBAL\Driver\PDODblib;

/**
 * PDODblib Connection implementation.
 *
 * @since 2.0
 */
class Connection extends \Doctrine\DBAL\Driver\PDOConnection implements \Doctrine\DBAL\Driver\Connection {

	protected $_pdoTransactionsSupport = null;
	protected $_pdoLastInsertIdSupport = null;
	/**
	 * @override
	 */
	public function quote($value, $type = \PDO::PARAM_STR) {
		$val = parent::quote($value, $type);

		// Fix for a driver version terminating all values with null byte
		if (strpos($val, "\0") !== false) {
			$val = substr($val, 0, -1);
		}

		return $val;
	}

	/**
	 * @return bool PDO_DBlib transaction support
	 */
	private function _pdoTransactionsSupported() {
		if (!is_null($this->_pdoTransactionsSupport)) {
			return $this->_pdoTransactionsSupport;
		}

		$supported = false;
		try {
			$supported = true;
			parent::beginTransaction();
		} catch (\PDOException $e) {
			$supported = false;
		}
		if ($supported) {
			parent::commit();
		}

		return $this->_pdoTransactionsSupport = $supported;
	}

	/**
	 * {@inheritdoc}
	 */
	public function rollback() {
		if ($this->_pdoTransactionsSupported() === true) {
			parent::rollback();
		} else {
			$this->exec('ROLLBACK TRANSACTION');
		}
	}

	/**
	 * {@inheritdoc}
	 */
	public function commit() {
		if ($this->_pdoTransactionsSupported() === true) {
			parent::commit();
		} else {
			$this->exec('COMMIT TRANSACTION');
		}
	}

	/**
	 * {@inheritdoc}
	 */
	public function beginTransaction() {
		if ($this->_pdoTransactionsSupported() === true) {
			parent::beginTransaction();
		} else {
			$this->exec('BEGIN TRANSACTION');
		}
	}

	/**
	 * @return bool PDO_DBlib::lastInsertId support
	 */
	private function _pdoLastInsertId() {
		if (!is_null($this->_pdoLastInsertIdSupport)) {
			return $this->_pdoLastInsertIdSupport;
		}

		$supported = false;
		try {
			$supported = true;
			parent::lastInsertId();
		} catch (\PDOException $e) {
			$supported = false;
		}

		return $this->_pdoLastInsertIdSupport = $supported;
	}

	/**
	 * {@inheritdoc}
	 */
	public function lastInsertId($name = null) {
		$id = null;
		if ($this->_pdoLastInsertId() === true) {
			$id = parent::lastInsertId();
		} else {
			$stmt = $this->query('SELECT SCOPE_IDENTITY()');
			$id = $stmt->fetchColumn();
			$stmt->closeCursor();
		}

		return $id;
	}

}
