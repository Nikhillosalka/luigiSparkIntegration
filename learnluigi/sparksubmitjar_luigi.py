import random

import luigi
import luigi.format
import luigi.contrib.hdfs
import subprocess
from luigi.contrib.spark import SparkSubmitTask


class SomeRandomTask(SparkSubmitTask):
    data_size = 1000

    def run(self):
        w = self.output().open('w')
        for user in range(self.data_size):
            track = int(random.random() * self.data_size)
            w.write('%d\%d\%f' % (user, track, 1.0))
        w.close()

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget('/user/Nikhil/data.txt')


class SparkSubmitToWriteFileToHDFS(SparkSubmitTask):
    spark_submit = "/usr/local/spark-2.1.1-bin-hadoop2.7/bin/spark-submit"
    deploy_mode = "client"
    master = "local[*]"
    entry_class = 'com.scala.spark.CreateFile'
    app = '/Users/Nikhil/Projects/MasterDataPiper/sparkbasics/target/sparkbasics-1.0-SNAPSHOT-jar-with-dependencies.jar'
    data_size = luigi.IntParameter(default=1000)
    driver_memory = '2g'
    executor_memory = '3g'
    num_executors = luigi.IntParameter(default=100)
    submit_command = luigi.parameter.Parameter(default="/usr/local/spark-2.1.1-bin-hadoop2.7/bin/spark-submit")
    executor_cores = luigi.parameter.Parameter(default="1")
    max_executors = luigi.parameter.Parameter(default="2")
    hdfs_root = 'hdfs://localhost:9000'

    def app_options(self):
        return [self.hdfs_root, self.output().path]

    def requires(self):
        return SomeRandomTask()

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget('/user/hdfs/wiki/testwiki')

    def run(self):
        super(SparkSubmitToWriteFileToHDFS, self).run()


class SparkSubmitToCountWords(SparkSubmitTask):
    spark_submit = "/usr/local/spark-2.1.1-bin-hadoop2.7/bin/spark-submit"
    deploy_mode = "client"
    master = "local[*]"
    entry_class = 'com.scala.spark.WordCount'
    app = '/Users/Nikhil/Projects/MasterDataPiper/sparkbasics/target/sparkbasics-1.0-SNAPSHOT-jar-with-dependencies.jar'
    data_size = luigi.IntParameter(default=1000)
    driver_memory = '2g'
    executor_memory = '3g'
    num_executors = luigi.IntParameter(default=100)
    submit_command = luigi.parameter.Parameter(default="/usr/local/spark-2.1.1-bin-hadoop2.7/bin/spark-submit")
    executor_cores = luigi.parameter.Parameter(default="1")
    max_executors = luigi.parameter.Parameter(default="2")
    hdfs_root = 'hdfs://localhost:9000'

    def app_options(self):
        return [self.hdfs_root, self.input().path, self.output().path]

    def requires(self):
        return SparkSubmitToWriteFileToHDFS()

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget('/user/hdfs/wiki/testwikiResult')

    def run(self):
        super(SparkSubmitToCountWords, self).run()


class JavaProcessDownloadFileFromHdfs(luigi.Task):
    hdfs_root = 'hdfs://localhost:9000'
    outputpath = '/Users/Nikhil/Desktop/download'

    def requires(self):
        return SparkSubmitToCountWords()

    def run(self):
        subprocess.call(['java', '-cp',
                         '/Users/Nikhil/Projects/MasterDataPiper/sparkbasics/target/sparkbasics-1.0-SNAPSHOT-jar-with-dependencies.jar',
                         'com.java.spark.DownloadFile',self.hdfs_root,self.input().path,self.outputpath])

if __name__ == '__main__':
    luigi.run()
