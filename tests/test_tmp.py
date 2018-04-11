import pandas as pd
from d6t.stack.helpers import apply_select_rename

def test_apply_select_rename():
    df1 = pd.DataFrame({'a':range(10)})
    df2 = pd.DataFrame({'b': range(10)})
    assert df1.equals(apply_select_rename(df2.copy(),[],{'b':'a'}))
    df2 = pd.DataFrame({'b': range(10),'c': range(10)})
    assert df1.equals(apply_select_rename(df2.copy(),['b'],{'b':'a'}))
    # assert df1.equals(apply_select_rename(df2.copy(),['a'],{'b':'a'}))

test_apply_select_rename()