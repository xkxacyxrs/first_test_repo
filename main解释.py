from psutil import disk_partitions    #从第三方库获取当前系统的磁盘分区信息
import time    #引入时间模块
import re      #引入re模块，用于寻找指定字符串
import subprocess     #引入subprocess模块，允许你去创建一个新的进程让其执行另外的程序，并与它进行通信，获取标准的输入、标准输出、标准错误以及返回码等
from subprocess import Popen     #从subprocess引入Popen类，通过它创建进程
import os      #引入OS模块，python环境下对文件，文件夹执行操作的一个模块
from fds import GalaxyFDSClient, GalaxyFDSClientException, FDSClientConfiguration
import shutil     #引入shutil模块，高级的文件，文件夹，压缩包的处理模块,也主要用于文件的拷贝
from multiprocessing import Pool    #提供多进程供调用
import threading     #引入多线程模块
import schedule      #引入定时模块
from loguru import logger     #日志管理模块


client = GalaxyFDSClient(
    access_key="AK5NTGOUKNLNJGWFAB",
    access_secret="zoT8W9NAyEtSPjP/wsqQnhHPYm0kU9Yp+v1jRqQJ",
    config=FDSClientConfiguration(
        endpoint="cnbj1.fds.api.xiaomi.com",
        enable_cdn_for_upload=False,
        enable_cdn_for_download=False,
    ),
)

data_disk = []
history_folder = []
thread_list = []


def format_disk(disk_path: str):
    process = subprocess.Popen(['cmd', 'format D: /Q '],
                               shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    for line in process.stdout:
        print(line)


# def compress_dir(compress_folder_and_filename):
#     dir_path, compress_name = compress_folder_and_filename
#     compressed_filename = os.path.join(dir_path, compress_name)
#     if os.path.isfile(compressed_filename):
#         try:
#             if tarfile.is_tarfile(compressed_filename):
#                 return compressed_filename
#         except BaseException as e:
#             logger.error(f"exists {compress_folder_and_filename} is not compress file, reason is {e}")
#     try:
#         name = shutil.make_archive(compressed_filename, "tar", dir_path)
#         logger.info(f"compress {compress_folder_and_filename} success, res is {name}")
#         return name
#     except BaseException as e:
#         logger.error(f"compress {compress_folder_and_filename}, reason is {e}")


def upload_check(fsd_object_name, file_size):
    try:
        res = client.get_object_metadata("micar-pnc", fsd_object_name)
        if res.metadata.get('x-xiaomi-meta-content-length') == str(file_size):
            logger.info(f"check {fsd_object_name} upload success")
            return True
    except GalaxyFDSClientException as e:
        logger.error(f"{fsd_object_name} upload error, reason is {e}")
    return False


def copy_file_to_hdd(file_path):
    logger.info(f"copy {file_path} to hdd")
    fds_object_regex = re.compile(".*(\d\d\d\d-\d\d-\d\d/\d\d-\d\d-\d\d.*)")
    object_name = re.findall(fds_object_regex, file_path)
    if not object_name:
        return False
    fsd_object_name = object_name[0]          #[0]取第一个元素
    path_split = os.path.split(fsd_object_name)
    if not path_split:
        return False
    des_path = '/mnt/hdd/pnc/record/' + path_split[0]   #分割取年月日命名文件夹
    if not os.path.isdir(des_path):
        os.makedirs(des_path)
    try:
        shutil.copy(file_path, des_path)
    except OSError as e:
        logger.error("copy Error copy: %s - %s." % (e.filename, e.strerror))
        return False
    return True


def rm_uploded_file(file_path):
    logger.info(f"del {file_path}")
    try:
        if os.path.isfile(file_path):
            os.remove(file_path)
    except BaseException as e:
        logger.error(f"delete {file_path} error ,reason is {e}")
        return False
    return True


def upload_file_to_fsd(file_path):
    logger.info(f"start to upload {file_path} to fds")
    fds_object_regex = re.compile(".*(\d\d\d\d-\d\d-\d\d/\d\d-\d\d-\d\d.*)")
    _ = re.findall(fds_object_regex, file_path)
    if not _:
        return None
    fsd_object_name = _[0]
    size = os.path.getsize(file_path)
    if upload_check(fsd_object_name, size):
        return file_path

    try:
        with open(file_path, "rb") as f:                     #rb，按照二进制读取  
            try:
                client.put_object("micar-pnc", fsd_object_name, f)
            except GalaxyFDSClientException as e:
                logger.error(f"upload error, reason is {e}")
    except BaseException as e:
        logger.error(f"open upload error, reason is {e}")

    if upload_check(fsd_object_name, size):
        return file_path
    return None


def get_upload_files(dir_path_list):
    upload_folder = []
    upload_files = []

    for dir_path in dir_path_list:
        for root, dirs, files in os.walk(dir_path):          #os.walk,在以top为根的目录树中游走，对树中的每个目录生成一个有目录路径、目录名、文件名的三元组
            if "record" in files and "$" not in root and ".Trash" not in root:
                upload_folder.append(root)

    for folder in upload_folder:
        for root, dirs, files in os.walk(folder):
            if files:
                for file in files:
                    file_path = os.path.join(root, file)     #os.path.join函数，连接两个或多个路径名组件，组件名不包含/，会自动加上，如果组件为绝对路径，则会舍弃前面的所有组件，如果最后一个组件为空，会生成路径以/结尾
                    upload_files.append(file_path)
    return upload_files


def find_disk():
    ssd_uuid = ['DA80-CF83', '3ec97794-868a-4515-a874-f86f391bc866', 'ae549407-d62a-4c90-ab7d-80c4f0ad5b7f', 'ac35c9cd-c3c5-4317-b1ef-067096c6279d']
    for ssd in ssd_uuid:
        address = "/media/" + ssd
        if os.path.exists("/dev/disk/by-uuid/" + ssd):
            # if not os.path.ismount(address):
            #     try:
            #         p = Popen(f"sudo mount /dev/disk/by-uuid/{ssd} {address}", shell=True)
            #     except BaseException as e:
            #         data_disk.remove(address)
            #         logger.error(f"mount error, reason is {e}")
            if os.path.ismount(address):
                if address not in data_disk:
                    data_disk.append(address)
            else:
                logger.error(f"mount {ssd} error")
                if address in data_disk:
                    data_disk.remove(address)
        else:
            if address in data_disk:
                data_disk.remove(address)
    logger.info(data_disk)

# def compress_all_file():
#     global history_folder
#     if not data_disk:
#         history_folder.clear()
#         return

#     upload_folder = get_upload_file_dir(data_disk)
#     diff_folder = set(upload_folder) - set(history_folder)
#     logger.info(f"the upload folder diff is {diff_folder}")
#     history_folder = upload_folder
#     diff_folder = list(diff_folder)
#     for folders in [diff_folder[i:i + 8] for i in range(0, len(diff_folder), 8)]:
#         logger.info(f"start compress folder is {folders}")
#         with Pool(processes=8) as pool:
#             compress_list = pool.map(compress_dir, folders)
#             logger.info(f"compress success, file is {compress_list}")
#             for compress_file in compress_list:
#                 for folder in folders:
#                     dir_path, compress_name = folder
#                     if os.path.splitext(os.path.basename(compress_file))[0] == compress_name:
#                         upload_q.put((dir_path, compress_file))


def upload_thread():
    if not data_disk:
        logger.error("no data disk")
        return
    for i in thread_list:
        if i.name == "upload" and i.is_alive():
            logger.info("thread is running")
            return
    t = threading.Thread(target=background_upload, name="upload")
    t.start()
    thread_list.append(t)


# def background_compress():
#     for i in compress_thread:
#         if i.name == "compress" and i.is_alive():
#             return
#     t = threading.Thread(target=compress_all_file, name="compress")
#     t.start()
#     compress_thread.append(t)


def background_upload():
    if not data_disk:
        logger.error("no disk")
        return
    
    upload_files = get_upload_files(data_disk)
    
    for upload_files_8 in [upload_files[i:i + 8] for i in range(0, len(upload_files), 8)]:
        with Pool(processes=8) as pool:
            uploaded_files = pool.map(upload_file_to_fsd, upload_files_8)
            for uploaded_file in uploaded_files:
                if uploaded_file is not None:
                    if copy_file_to_hdd(uploaded_file):
                        rm_uploded_file(uploaded_file)


if __name__ == '__main__':         #当.py文件被直接运行时，if __name__ == '__main__'之下的代码块将被运行；当.py文件以模块形式被导入时，if __name__ == '__main__'之下的代码块不被运行。
    logger.add(
        "file_{time}.log", format="[{time}] [{level}] - {name}:{function}:{line} - {message}" , rotation="10 MB")
    schedule.every(3).seconds.do(find_disk)
    schedule.every(5).seconds.do(upload_thread)
    while True:
        schedule.run_pending()
        time.sleep(1)

