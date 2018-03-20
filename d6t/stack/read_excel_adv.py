from pandas import ExcelFile


def read_excel_adv(io, sheet_name=0, header=0, index_col=None, usecols=None, names=None, parse_dates=False,
                   date_parser=None, na_values=None, thousands=None, convert_float=True, converters=None,
                   dtype=None, true_values=None, false_values=None, engine=None, squeeze=False,
                   userows=None, remove_blank_cols=False, remove_blank_rows=False, collapse_header=False,
                   header_range=None, **kwds):
    """
    # TODO: Handle multiple sheets at once. Each sheet may have difference col and rows range.
    :param userows: string for a single dataset(eg '1:100' means row 1 to row 100)
    :param remove_blank_cols: to remove all blank columns
    :param remove_blank_rows: to remove all blank rows
    :param collapse_header: Replace newline ('\n') to space
    :return: dataframe
    """
    if not isinstance(io, ExcelFile):
        io = ExcelFile(io, engine=engine)

    skiprows = 0
    skip_footer = 0
    if userows:
        userows = [int(x) for x in userows.split(':')]
        if sheet_name:
            total_rows = io.book.sheet_by_name(sheet_name).nrows
        else:
            total_rows = io.book.sheet_by_index(0).nrows
        print(total_rows)
        skiprows = userows[0] - 1
        last_row = userows[1]
        # calc number of footer rows
        skip_footer = total_rows - last_row

    df = io.parse(
        sheet_name=sheet_name, header=header, skiprows=skiprows, names=names,
        index_col=index_col, usecols=usecols, parse_dates=parse_dates,
        date_parser=date_parser, na_values=na_values, thousands=thousands,
        convert_float=convert_float, skip_footer=skip_footer,
        converters=converters, dtype=dtype, true_values=true_values,
        false_values=false_values, squeeze=squeeze, **kwds)
    if remove_blank_cols:
        df = df.dropna(axis='columns', how='all')
    if remove_blank_rows:
        df = df.dropna(axis='rows', how='all')
    if collapse_header:
        df.rename(columns=lambda x: x.strip().replace("\n", ' '), inplace=True)
    return df
