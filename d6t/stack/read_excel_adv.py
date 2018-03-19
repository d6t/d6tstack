from pandas import ExcelFile


def read_excel_adv(io, sheet_name=0, index_col=None, usecols=None, names=None, parse_dates=False,
                   date_parser=None, na_values=None, thousands=None, convert_float=True, converters=None,
                   dtype=None, true_values=None, false_values=None, engine=None, squeeze=False,
                   userows=None, remove_blank_cols=False, collapse_header=False, header_range=None,
                   **kwds):
    """
    # TODO: Handle multiple sheets at once. Each sheet may have difference col and rows range.
    :param names: TODO
    :param userows: string for a single dataset(eg '1:100' means row 1 to row 100)
    :param remove_blank_cols:
    :param collapse_header: Replace newline ('\n') to space
    :param header_range: list of header range (list of list/int) or a single list of headers or an int
                          (None for default)
    :return:
    """
    if not isinstance(io, ExcelFile):
        io = ExcelFile(io, engine=engine)

    header = 0
    if header_range:
        header = header_range

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
    if collapse_header:
        df.rename(columns=lambda x: x.strip().replace("\n", ' '), inplace=True)
    return df
