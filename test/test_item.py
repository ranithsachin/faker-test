from src.items import Item
import pytest
import conf.configs as config

@pytest.fixture
def item()->Item:
    """Return item instance"""
    return Item()

def test_item_data_count(item:Item)->None:
    """
    Testing the item record count equals to the number defined in configs

    Args:
        item: Item object
    """
    item_data = item.read_items()
    assert item_data.count()==config.NO_OF_ITEMS
