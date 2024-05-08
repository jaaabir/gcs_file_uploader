import json 
import os 
from gcs_file_uploader import GcsFileUploader
from glob import glob
from tqdm import tqdm
from time import sleep

def read_json(jpath):
    with open(jpath, 'r') as raw_data:
        data = json.load(raw_data)
    return data

def write_json(data, jpath):
    with open(jpath, 'w') as file:
        json.dump(data, file)

def merge_filepath(filepath, st_index = 1, replace_string = '_', delimiter = '\\'):
    filename = filepath.split(delimiter)[st_index:]
    filename = f'{replace_string}'.join(filename)
    return filename

def main(glob_img_path, glob_txt_path, bucket_key_path):
    global num_of_samples_done, N

    gfu = GcsFileUploader(bucket_key_path = bucket_key_path)

    num_of_samples_done = read_json('num_of_files_done.json')['st_index']
    images = glob(glob_img_path)
    labels = glob(glob_txt_path)
    N = len(images)

    if num_of_samples_done != 0:
        print(f'Starting data transfer from file number {num_of_samples_done}')

    for i in tqdm(range(num_of_samples_done, N)):

        image_path = images[i]
        label_path = labels[i]
        dest_iname = GcsFileUploader.merge_root_fname('images', merge_filepath(image_path, 3))
        dest_lname = GcsFileUploader.merge_root_fname('labels', merge_filepath(label_path, 3))
        gfu.upload_file(image_path, dest_iname)
        gfu.upload_file(label_path, dest_lname)
        num_of_samples_done = i + 1

    print('SUCCESS')


if __name__ == "__main__":
    ## initialization 
    glob_img_path = r'hand_tracker\syn_hand\SynthHands_Release\*\*\*\*\*_color_on_depth.*'
    glob_txt_path = r'hand_tracker\syn_hand\SynthHands_Release\*\*\*\*\*.txt'
    bucket_key_path = os.path.join('creds', 'gkey.json')
    

    try:
        main(glob_img_path, glob_txt_path, bucket_key_path)
    except KeyboardInterrupt as err:
        print('Data transfer stopped ...')
        print(f'Transfered {num_of_samples_done}/{N} files to the bucket ...')
        write_json({'st_index' : num_of_samples_done}, 'num_of_files_done.json')

