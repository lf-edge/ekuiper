import sys
import json

version_list = sys.argv[1:]
if len(version_list) == 1:
    version_list = version_list[0].split(' ')


if __name__ == '__main__':
    version_list = ['.'.join(version.split('.')[:2]) for version in version_list]
    version_list = [version[1:] for version in version_list if version.startswith('v')]
    version_list = list(set(version_list))
    version_list.sort(
        key=lambda v: [int(u) for u in v.split('.')],
        reverse=True
    )
    version_list = [f'v{version}' for version in version_list]
    version_list.insert(0, 'latest')
    print(json.dumps(version_list))
