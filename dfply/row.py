from pyrsistent import PRecord, field, freeze, pmap

class Row(PRecord):
    """Base class for the rows in a data frame.  
    
       This class will be used to dynamically create new classes for rows, which
       have properties for each column.  By inheriting from PRecord, we can leverage 
       the following functionality.
       
       1. types and factories: We will be able to create factories and types to easily handle column types.
       2. Easy interaction with Intentions.  For example, X.col1 will evaluate to the value of col1.
       3. COMING SOON: Define invariants for columns (e.g. must be positive or between 0 and 1)
       
       """
    def _maybe_apply(self, column, func, none_as_default = True):
        """ Columns may contain missing data (encoded as None), 
        the method acts as the fmap for the Maybe Functor by returning
        None if the current entry is None, otherwise returning func(current_value)
        
        By default (none_as_default) this will also return None if the column doesn't exist."""
        
        if none_as_default and column not in self:
            return None
        old_value = self[column]
        return None if old_value is None else func(old_value)
    
    
    def adjust(self, column, func):
        """ Apply func to the current value in column.
        
        This method works like map in the Maybe monad, e.g. func(None) returns None."""
        new_value = self._maybe_apply(column, func, none_as_default=False)
        return self.set(**{column:new_value})
    
    
    def adjust_with(self, **kwargs):
        """ Apply each func in kwargs to the current value in corresponding column.
        
        This method works like map in the Maybe monad, e.g. func(None) returns None."""
        assert all(callable(val) for val in kwargs.values()), "The keyword arguments need to all be row functions" 
        return self.set(**{col:self._maybe_apply(col, func, none_as_default=False) 
                           for col, func in kwargs.items()})
     

    def alter(self, column, func, new_type = str):
        """ Apply func to the current row, and create a new column containing this value.
        
        This method works like bind in the Maybe monad, So you need to provide a function that can
        deal with possible missing/None values (i.e. func(row) -> Optional[a].)
        
        NOTE: Alteration changes the row structure, so we return a raw dictionary.  A new Row constructor
        will need to be created and employed at the data frame level.
        
        kwargs:
        - use the keyword new_type to set the type, otherwise the type will be str (for safey sake).
        - (COMING SOON) use the keyword invariant to set an invariant on the new column"""
        new_value = func(self)
        val_dict = self.serialize()
        val_dict[column] = new_value
        return pmap(val_dict)
    
    
    def alter_with(self, new_types = {}, **kwargs):
        """ Apply each func in kwargs to the current row and creating new columns as needed.
        
        This method works like multiple applications of bind in the Maybe monad, 
        So you need to provide a functions that can deal with possible missing/None values 
        (i.e. func(row) -> Optional[a].) """
        assert all(callable(val) for val in kwargs.values()), "The keyword arguments need to all be row functions" 
        new_values = {col:f(self) for col, f in kwargs.items()}
        return pmap(self).update(new_values)
        
        
    def select_with_predicate(self, pred):
        """ Keep all columns for which the predicate is True, discarding the others."""
        out = self
        for key in self:
            if not pred(key):
                out =  out.discard(key)
        return out
    
    
    def select(self, columns_to_keep):
        """ Keep all columns in columns_to_keep, discarding the others."""
        out = self
        for key in self:
            if key not in col:
                out =  out.discard(key)
        return out

    
def col_factory(type_constructor):
    if type_constructor.__name__ == 'str':
        return lambda val: val if val is not None and len(val) > 0 else None
    else:
        def factory(val):
            try:
                return type_constructor(val)
            except ValueError:
                return None
        return factory
    

def get_col_types(names, col_type_dict):
    return [col_type_dict.get(name, str) for name in names]


def get_field(col_type, **kwargs):
    return field(type=(col_type, type(None)),
                 factory = col_factory(col_type),
                 initial = None,
                 **kwargs)

    
def columns_and_types(names, col_type_dict):
    return zip(names, get_col_types(names, col_type_dict))


def row_fields(names, col_type_dict, kwarg_dict={}):
    return {name:get_field(col_type, **kwarg_dict.get(name, {})) 
            for name, col_type in columns_and_types(names, col_type_dict) 
            if name.isidentifier()}


def _make_unique_name(base):
    n = 0
    def col_maker():
        nonlocal n
        n += 1
        return base + str(n)
    return col_maker


_col_name = _make_unique_name("Column")
_row_name = _make_unique_name("Row")


def make_row_class(names, col_type_dict, field_kwargs = {}):
    return type(_row_name(), (Row,), row_fields(names, col_type_dict, kwarg_dict=field_kwargs))