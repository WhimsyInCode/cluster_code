import subprocess

HADOOP_STREAMING_JAR_PATH = "/usr/lib/hadoop/hadoop-streaming.jar"

def run_command(cmd):
    print("Trying to run:\n{}".format(cmd))
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True
        )
        print("Command output:\n{}".format(result))
        return result.returncode, result.stdout.strip(), result.stderr.strip()
    except Exception as e:
        return -1, "", f"Error when executing: {e}"

def submit_hadoop_job(mapper_path, reducer_path, input_path, output_path):
    command = "hadoop jar {} -file {} -mapper \'python {}\' -file {} -reducer \'python {}\' -input {} -output {}".format(
        HADOOP_STREAMING_JAR_PATH,
        mapper_path,
        mapper_path,
        reducer_path,
        reducer_path,
        input_path,
        output_path
    )

    ret_value = run_command(command)
    return ret_value

def upload_file_to_hdfs(file_path, dest_path):
    command = "hadoop fs -put {} {}".format(file_path, dest_path)
    ret_value = run_command(command)
    return ret_value

def build_index(index_id):
    """
    input_data_path: ./{index_id}
    """
    upload_file_to_hdfs("./{}".format(index_id), "/")
    return submit_hadoop_job("mapper.py", "reducer.py", "/{}".format(index_id), "/{}-TempOutFolder".format(index_id))

def merge_output(index_id):
    command = "hadoop fs -getmerge {} {}".format("/{}-TempOutFolder".format(index_id), "./{}-index".format(index_id))
    return run_command(command)