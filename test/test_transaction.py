from src.transactions import Transaction
import pytest as pytest
import conf.configs as config

@pytest.fixture
def trans()->Transaction:
    """Return transaction instance"""
    return Transaction()

def test_product_transaction_count(trans:Transaction)->None:
    """
    Testing the product transactions count equals to the number defined in configs

    Args:
        trans: Transaction object
    """
    prod_trans = trans.read_product_trans()
    assert prod_trans.count()==config.NO_OF_TRANSACTIONS

def test_creditcard_transaction_count(trans:Transaction)->None:
    """
    Testing the creditcard transactions count equals to the number defined in configs

    Args:
        trans: Transaction object
    """
    cc_trans = trans.read_cc_trans()
    assert cc_trans.count()==config.NO_OF_TRANSACTIONS


