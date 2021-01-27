import os
import sys
import json
import re

docs_path = sys.argv[1]
success = True

def check_path(path_list, folder):
    global success
    for i in path_list:
        md_path = i.get('path')
        md_children = i.get('children')
        if md_path and md_children:
            print(f'{i.get("title")} has path and children')
            success = False
        if md_children:
            check_path(md_children, folder)
        else:
            if md_path.startswith(('http://', 'https://')) or md_path == './':
                continue
            file_path = f'{docs_path}/{folder}/{md_path}.md'

            if not os.path.exists(file_path):
                print(f'{folder}/{md_path}.md not exists')
                success = False
                continue

            md_content = open(file_path, 'r').read()
            image_list = re.findall('(.*?)!\[(.*?)\]\((.*?)\)', md_content)
            for image in image_list:
                if image[0].startswith('<!--'):
                    continue
                if image[2].startswith(('http://', 'https://', '<')):
                    continue
                image_path = os.path.join(f'{"/".join(file_path.split("/")[:-1])}/', image[2])

                if not os.path.exists(image_path):
                    print(f'In {folder}/{md_path}.md：', end='')
                    print(image[2], 'does not exist')
                    success = False


if __name__ == '__main__':
    file_list = []
    if os.path.exists(f'{docs_path}/directory.json'):
        file_list.append('directory.json')

    for file in file_list:
        with open(f'{docs_path}/{file}') as f:
            print(f'Check {file}...')
            config_dict = json.load(f)
            check_path(config_dict['cn'], 'zh_CN')
            check_path(config_dict['en'], 'en_US')

    if not success:
        sys.exit('No pass!')
    else:
        print('Check completed!')
