import pytest

from dfply.columns import Columns, _column_fields, _make_column_input, make_columns
from pyrsistent._field_common import _PField


columns = ['Salesperson', 'Compact', 'Sedan', 'SUV', 'Truck']
c1 = Columns(all_columns=columns, current_set=columns)
c2 = Columns(all_columns=columns, current_set=columns)
c3 = Columns(all_columns=columns, current_set=pset(['Truck','SUV']))


def check_membership(items, cols):
    return all(i in items for i in cols)


def check_equality(items, cols):
    return pset(items) == pset(cols)


def test_column_not_equal_operator():
    assert not c1 != c1
    assert not c1 != c2
    assert c1 != c3
    
    
def test_column_symmetric_diff_operator():
    assert check_membership(['Compact', 'Salesperson', 'Sedan'], c1 ^ c1)
    assert check_equality([],  c1 ^ c2)
    assert check_equality(['Compact', 'Salesperson', 'Sedan'], c1 ^ c3)
    
    
def test_column_invert_operator():
    assert check_equality(['Compact', 'Truck', 'Salesperson', 'Sedan', 'SUV'], ~(c1 ^ c1))
    assert check_equality(['Compact', 'Truck', 'Salesperson', 'Sedan', 'SUV'], ~(c1 ^ c2))
    assert check_equality(['Truck', 'SUV'], ~(c1 ^ c3))

    
def test_column_intersection_operator():
    assert check_equality(columns, c1 & c1)
    assert check_equality(columns, c1 & c2)
    assert check_equality(['Truck', 'SUV'], c1 & c3)

    
def test_column_difference_operator():
    assert check_equality([], c1 - c1)
    assert check_equality([], c1 - c2)
    assert check_equality(['Compact', 'Salesperson', 'Sedan'], c1 - c3)
    assert check_equality(['Truck', 'SUV'], ~(c1 - c3))
    
    
def test_col_fields():
    col_fields = _column_fields(columns)
    assert check_membership(columns, list(col_fields.keys()))
    assert all(isinstance(v, _PField) for v in col_fields.values())
    
    
def test_col_input():
    col_input = _make_columns_input(columns)
    assert col_input['all_columns'] == columns
    assert col_input['current_set'] == columns
    assert all(col_input[k] == k for k in columns)
    
    
def test_columns_field():
    cols = make_columns(columns)
    assert check_membership(columns, cols.Salesperson.all_columns)
    assert check_equality(['Salesperson'], cols.Salesperson.current_set)

def test_field_operations():
    cols = make_columns(columns)
    out1 = cols.Salesperson + cols.Truck
    out2 = ~(cols.Salesperson + cols.Truck)
    assert all(check_equality(columns, o.all_columns) for o in (out1, out2))
    assert check_equality(['Truck', 'Salesperson'], out1.current_set)
    assert check_equality(['Compact', 'Sedan', 'SUV'], out2.current_set)