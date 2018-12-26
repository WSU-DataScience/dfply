from pyrsistent import pmap, pset, pset_field, pvector_field, PRecord, field


__all__ = ["Columns",
           "make_columns"]

_COMPARISON_METHODS = ['__ne__',
                       '__ge__', 
                       '__lt__', 
                       '__eq__', 
                       '__gt__', 
                       'issuperset', 
                       'issubset',
                       '__contains__', 
                       'isdisjoint',
                       '__le__']

_UNARY_OPERATIONS = ['__len__', 
                     '__sizeof__'] 

_SET_OPERATIONS = ['__and__', 
                   '__sub__', 
                   '__or__',  
                   '__xor__', 
                   'union', 
                   'intersection', 
                   'difference', 
                   'symmetric_difference', 
                   'update']


def wrap_comparison_method(method_name):
    def magic_method(self, other):
        if isinstance(other, PRecord) and hasattr(other, 'current_set'): 
            return getattr(self.current_set, method_name)(other.current_set)
        else:
            return getattr(self.current_set, method_name)(other)
    return magic_method


def wrap_unary_method(method_name):
    def magic_method(self):
        return getattr(self.current_set, method_name)()
    return magic_method


def wrap_set_operations(method_name):
    def magic_method(self, other):
        if isinstance(other, PRecord) and hasattr(other, 'current_set'): 
            current_set = getattr(self.current_set, method_name)(other.current_set)
        else:
            current_set = getattr(self.current_set, method_name)(other)
        return self.__class__(all_columns = self.all_columns, current_set=current_set)
    return magic_method


def column_invert(self):
    current_set = pset(self.all_columns) - self.current_set
    return self.__class__(all_columns=self.all_columns, current_set=current_set)


class Columns(PRecord):
    all_columns = pvector_field(str)
    current_set = pset_field(str)
    
    
for method in _COMPARISON_METHODS:
    setattr(Columns, method, wrap_comparison_method(method))

for method in _UNARY_OPERATIONS:
    setattr(Columns, method, wrap_unary_method(method))
    
for method in _SET_OPERATIONS:
    setattr(Columns, method, wrap_set_operations(method))
    
setattr(Columns, '__invert__', column_invert)
setattr(Columns, '__iter__', lambda self: self.current_set.__iter__())



def _make_unique_column_name():
    n = 0
    def col_maker():
        nonlocal n
        n += 1
        return 'Column' + str(n)
    return col_maker


_col_name = _make_unique_column_name()


def _column_fields(columns):
    return {n:field(factory=lambda s: Columns(all_columns=columns, current_set=pset([s]))) 
            for n in columns if n.isidentifier()}


def _make_column_type(columns):
    return type(_col_name(), (Columns, ), _column_fields(columns))


def _make_columns_input(columns):
    output = pmap({'all_columns':columns,
                   'current_set':columns})
    return output.update({n:n for n in columns})


def make_columns(columns):
    return _make_column_type(columns)(**_make_columns_input(columns))