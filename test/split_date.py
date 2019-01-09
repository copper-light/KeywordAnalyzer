from datetime import datetime, timedelta
import calendar
import pandas


# 주단위로 짜르기 월요일의 첫 데이터일 때, 새로운 분류 시작

#
SPLIT_TYPE_DAY = "DAY"
SPLIT_TYPE_WEEK = "WEEK"
SPLIT_TYPE_FIFTEEN_DAYS = "FIFTEEN_DAYS"
SPLIT_TYPE_MONTH = "MONTH"

def split_to_date(type, data, dateformat = "%Y.%m.%d"):
    ret = []
    predate = ""
    newdata = []
    newSection = []
    # calendar.monthrange(year, month)[1]

    # ret = [
    #   line[],
    #   line[],
    #   line[]
    # ]

    start_date = end_date = None
    for line in data:
        strdate = line
        curdate = datetime.strptime(strdate, dateformat)
        #
        # end_date = weekday + timedelta(days=gap_day)
        #
        # end_date = weekday + 15

        if type is SPLIT_TYPE_DAY:
            if start_date is end_date is None:
                start_date = end_date = curdate
                # 초기화
                newSection = []
            else:
                if end_date < curdate:
                    # 새로운 배열
                    ret.append({"start":start_date.strftime(dateformat), "end":end_date.strftime(dateformat), "type":type, "data":newSection})
                    end_date = start_date = curdate
                    newSection = []

        if type is SPLIT_TYPE_WEEK:
            if start_date is end_date is None: # 첫시작일부터 7일 단위로 연산
                start_date = curdate
                end_date = start_date + timedelta(days=6)
                # 초기화
                newSection = []
            else:
                if end_date < curdate:
                    # 새로운 배열
                    ret.append(
                        {"start": start_date.strftime(dateformat), "end": end_date.strftime(dateformat), "type": type,
                         "data": newSection})

                    gap_day = int((curdate - end_date).days / 7)
                    start_date = curdate + timedelta(days=gap_day)
                    end_date = start_date + timedelta(days=6)

                    newSection = []

        elif type is SPLIT_TYPE_FIFTEEN_DAYS:
            if start_date is end_date is None: # 1일-15일, 16일-마지막일
                base_date = datetime(curdate.year, curdate.month, 15)
                if base_date >= curdate: # 1일- 15일
                    start_date = datetime(curdate.year, curdate.month, 1)
                    end_date = datetime(curdate.year, curdate.month, 15)
                else:
                    start_date = datetime(curdate.year, curdate.month, 16)
                    end_date = datetime(curdate.year, curdate.month, calendar.monthrange(curdate.year, curdate.month)[1])
                #초기화
                newSection = []
            else:
                if end_date < curdate:
                    # 새로운 배열
                    ret.append({"start": start_date.strftime(dateformat), "end": end_date.strftime(dateformat),
                                "type": type, "data": newSection})

                    base_date = datetime(curdate.year, curdate.month, 15)
                    if base_date >= curdate:  # 1일- 15일
                        start_date = datetime(curdate.year, curdate.month, 1)
                        end_date = datetime(curdate.year, curdate.month, 15)
                    else:
                        start_date = datetime(curdate.year, curdate.month, 16)
                        end_date = datetime(curdate.year, curdate.month,calendar.monthrange(curdate.year, curdate.month)[1])

                    newSection = []

        elif type is SPLIT_TYPE_MONTH:
            if start_date is end_date is None:
                start_date = datetime(curdate.year, curdate.month, 1)
                end_date = datetime(curdate.year, curdate.month, calendar.monthrange(curdate.year, curdate.month)[1])
                # 초기화
                newSection = []
            else:
                if end_date < curdate:
                    # 새로운 배열
                    ret.append(
                        {"start": start_date.strftime(dateformat), "end": end_date.strftime(dateformat), "type": type,
                         "data": newSection})

                    start_date = datetime(curdate.year, curdate.month, 1)
                    end_date = datetime(curdate.year, curdate.month, calendar.monthrange(curdate.year, curdate.month)[1])

                    newSection = []

        newSection.append(line)
    if len(newSection) > 0:
        ret.append({"start":start_date.strftime(dateformat), "end":end_date.strftime(dateformat), "type":type, "data":newSection})

    return ret


dt_index = pandas.date_range(start='20180101', end='20181231')
dt_list = dt_index.strftime("%Y.%m.%d").tolist()

for line in split_to_date(SPLIT_TYPE_MONTH, dt_list):
    print(line)