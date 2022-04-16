from datetime import date
import pandas as pd
import time


def unixTimeMillis(dt):
    """ Convert datetime to unix timestamp """
    return int(time.mktime(dt.timetuple()))


def getMarks(start, end, Nth=100):
    """Returns the marks for labeling.
    Every Nth value will be used.
    """
    daterange = pd.date_range(start=start, end=end, freq="Y")
    result = {
        unixTimeMillis(date + pd.Timedelta(value=1, unit="D")): str(
            (date + pd.Timedelta(value=1, unit="D")).strftime("%Y")
        )
        for date in daterange
    }
    return result
