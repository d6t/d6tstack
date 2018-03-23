from pandas import read_excel
from openpyxl.utils import coordinate_from_string


def read_excel_adv(io, remove_blank_cols=False, remove_blank_rows=False, collapse_header=False,
                   header_range=None, header_xls_start=None, header_xls_end=None, **kwds):
    """
    # TODO: Handle multiple sheets at once. Each sheet may have difference col and rows range.
    :param header_xls_start: string for a single dataset(eg 'B23')
    :param header_xls_end: string for a single dataset(eg 'I24')
    :param header_range: string for a single dataset(eg 'B23:I24')
    :param remove_blank_cols: to remove all blank columns
    :param remove_blank_rows: to remove all blank rows
    :param collapse_header: Replace newline ('\n') to space
    :return: dataframe
    """
    if not (header_xls_start and header_xls_end):
        if header_range:
            header_range = header_range.split(':')
            header_xls_start , header_xls_end = header_range
        else:
            # Return with error message - Discuss with Norman
            raise Exception

    # header, skiprows, usecols
    scol, srow = coordinate_from_string(header_xls_start)
    ecol, erow = coordinate_from_string(header_xls_end)

    header = [x for x in range(erow - srow + 1)]
    usecols = scol + ":" + ecol
    skiprows = srow - 1

    df = read_excel(io, index_col=None, header=header, skiprows=skiprows, usecols=usecols, **kwds)

    if remove_blank_cols:
        df = df.dropna(axis='columns', how='all')
    if remove_blank_rows:
        df = df.dropna(axis='rows', how='all')
    if collapse_header:
        if len(header) > 1:
            df.columns = [' '.join([s for s in col if not 'Unnamed' in s]).strip().replace("\n", ' ')
                          for col in df.columns.values]
        else:
            df.rename(columns=lambda x: x.strip().replace("\n", ' '), inplace=True)
    return df
