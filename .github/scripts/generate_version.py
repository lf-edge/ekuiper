import sys
import json

version_list = sys.argv[1:]

if __name__ == '__main__':
    version_list.sort(
        key=lambda v: [int(u) for u in v.split('.')],
        reverse=True
    )
    version_list = [f'v{version}' for version in version_list]
    version_list.insert(0, 'latest')
    print(json.dumps(version_list))
