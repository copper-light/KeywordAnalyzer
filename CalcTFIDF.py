from konlpy.tag import Twitter
from MultiThreadWorker import MultiThreadWorker
import jpype
import datetime
import csv

ENCODE_TYPE = "utf-8"
TFDF_CSV_HEADER = ["index","date","keyword","fq","tf","tfdf","normal"]
DF_RP_CSV_HEADER = ["keyword","fq","df","rp"]

class TFIDFAnalyzer:
    def __init__(self):
        self.inputfile_path = ""
        self.dataindex=[]
        self.dateindex=0
        self.keyword_list = []
        self.thread_limit = 1
        self.pos_list=["Noun", "Adjective", "Exclamation"]
        self.keyword_list = {}
        self.twit = Twitter()

    def get_per_linetf(self, line_idx, line):
        tmp_time = datetime.datetime.now()
        ret_data = {}
        str_data = ""
        total_cnt = 0

        # twit = Twitter()
        jpype.attachThreadToJVM()

        for col_idx in self.dataindex:
            str_data += (line[col_idx] + " ")
        morpheme_list = self.twit.pos(str_data)
        for morpheme in morpheme_list:
            if morpheme[1] in self.pos_list:
                key = morpheme[0]
                total_cnt += 1
                if key in ret_data:
                    ret_data[key]["fq"] += 1
                else:
                    ret_data[key] = {"fq": 1}

        for key in ret_data:
            item = ret_data[key]
            item["tf"] = item["fq"] / total_cnt
            # print("calc tfdf", datetime.datetime.now() - tmp_time)
        return {"date": line[self.dateindex], "index": line_idx, "data": ret_data}

    def gettf_multithread(self, rdr):
        worker = MultiThreadWorker(limit=self.thread_limit, works=rdr, func=self.get_per_linetf)
        print("calc tf - use thread ", worker.getthreadsize())
        worker.setcheckterm(0)
        worker.start_and_wait()
        ret_tf = worker.getresult()
        return ret_tf

    def gettf(self, rdr):
        ret_tf = []
        for row_idx, line in enumerate(rdr):
            ret_tf.append(self.get_per_linetf(row_idx, line))

        return ret_tf

    def getdf(self, tf_list):
        df_ret = {}
        total_cnt = len(tf_list)
        for line in tf_list:
            tf = line["data"]
            for key in tf:
                if key in df_ret:
                    df_ret[key]["fq"] += 1
                else:
                    df_ret[key]={"fq": 1}

        for key in df_ret:
            item = df_ret[key]
            item["df"] = item["fq"] / total_cnt
            #print(key, item)
        return df_ret

    def gettfdf(self, tf_list, df_list):
        for line in tf_list:
            tf = line["data"]

            tfdf_sum = 0
            for key in tf:
                if key in df_list:
                    item = tf[key]
                    item["tfdf"] = item["tf"] * df_list[key]["df"] # calc tfdf
                    tfdf_sum += (item["tfdf"]**2)

            tfdf_sum = (tfdf_sum ** 0.5)
            for key in tf:
                item = tf[key]
                item["normal"] = item["tfdf"]/tfdf_sum

                if "normal_sum" in df_list[key]:
                    df_list[key]["normal_sum"] += item["normal"]
                else:
                    df_list[key]["normal_sum"] = item["normal"]

        for key in df_list:
            item = df_list[key]
            item["rp"] = item["normal_sum"] / item["fq"]
            item.pop("normal_sum", None)

        return tf_list, df_list

    def setposlist(self, list):
        if len(list) > 0 :
            self.pos_list = list

    def setinputfile(self, path, hasheader = True, dataindex= [1], dateindex = 0):
        self.inputfile_path= path
        self.dataindex= dataindex
        self.dateindex= dateindex
        self.hasheader = hasheader

    def outputtofile(self, output_tfdf="", output_df=""):
        if len(output_tfdf) == 0 or len(output_df) == 0:
            output_tfdf = self.inputfile_path+"tfdf.csv"
            output_df = self.inputfile_path + "df_rp.csv"

        f = open(output_tfdf, 'w', encoding=ENCODE_TYPE, newline='')
        wr = csv.writer(f)
        wr.writerow(TFDF_CSV_HEADER)

        for line in self.tf_list:
            date = line["date"]
            index = line["index"]
            data = line["data"]
            for key in data:
                item = data[key]
                wr.writerow([index, date, key, item["fq"], item["tf"], item["tfdf"], item["normal"]])
        f.close()

        f = open(output_df, 'w', encoding=ENCODE_TYPE, newline='')
        wr = csv.writer(f)
        wr.writerow(DF_RP_CSV_HEADER)

        for key in self.df_list:
            item = self.df_list[key]
            wr.writerow([key, item["fq"], item["df"], item["rp"]])
        f.close()

    def usemultithread(self, limit=2):
        if limit == 0:
            self.thread_limit = 1
        else:
            self.thread_limit = limit

    def analyze(self):
        print("start analyze", self.pos_list)
        with open(self.inputfile_path, encoding=ENCODE_TYPE) as f:
            rdr = csv.reader(f)
            if self.hasheader :
                next(rdr)


            print("open file", self.inputfile_path)

            tf_list = None
            df_list = None
            tmp_time = datetime.datetime.now()
            if self.thread_limit == 1:
                tf_list = self.gettf(rdr)
            else:
                tf_list = self.gettf_multithread(rdr)
            print("calc tf", datetime.datetime.now() - tmp_time)

            tmp_time = datetime.datetime.now()
            df_list = self.getdf(tf_list)
            print("calc df", datetime.datetime.now() - tmp_time)

            tmp_time = datetime.datetime.now()
            self.gettfdf(tf_list, df_list)
            print("calc tfdf", datetime.datetime.now() - tmp_time)

            self.tf_list = tf_list
            self.df_list = df_list

        return tf_list, df_list


start_time = datetime.datetime.now()
print()

analyzer = TFIDFAnalyzer()
analyzer.setinputfile("./data/sample.csv", hasheader=True, dataindex=[3,4], dateindex=1)
# analyzer.setinputfile("./data/naver_blog_01_10.csv", hasheader=True, dataindex=[1,2], dateindex=0)
analyzer.usemultithread(3)
ret = analyzer.analyze()
analyzer.outputtofile(output_tfdf="", output_df="")
print(datetime.datetime.now()-start_time)
