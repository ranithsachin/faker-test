import pytest as pytest

from src.customers import Customer
from src.read_write_data import ReadWriteData
import conf.configs as configs

@pytest.fixture
def cust()->Customer:
    """Return customer instance"""
    return Customer()

@pytest.fixture
def rw_data()->ReadWriteData:
    """Return read write data instance"""
    return ReadWriteData()

def test_customer_id_generator(cust:Customer)->None:
    """
    Testing the customer id generation function output

    Args:
        cust: Customer object
    """
    cust_id = cust.id_generator(5)
    assert cust_id == 'AAAA0005'

def test_customer_id_generator_except(cust:Customer)->None:
    """
    Testing the customer id generation raising of exception

    Args:
        cust: Customer object
    """
    with pytest.raises(Exception):
        cust_id=cust.id_generator()

def test_customer_data_count(cust:Customer)->None:
    """
    Testing the customer record count equals to the number defined in configs

    Args:
        cust: Customer object
    """
    customer = cust.read_customer_data()
    assert customer.count()==configs.NO_OF_CUSTOMERS

def test_customer_numbers_list(rw_data:ReadWriteData)->None:
    """
    Check the customer numbers list return type is a list
    Args:
        rw_data: ReadWriteData object
    """
    cust_list=rw_data.get_cust_number_list(configs.CUSTOMERS_TABLE_LOCATION)
    assert type(cust_list) is list

def test_customer_numbers_list_except(rw_data:ReadWriteData)->None:
    """
    Testing the customer number list method raising of exception
    Args:
        rw_data: ReadWriteData object
    """
    with pytest.raises(Exception):
        cust_list=rw_data.get_cust_number_list('test')

    
    


