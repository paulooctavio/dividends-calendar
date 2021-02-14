import yaml


def get_configs():
    configs = {}
    credentials = yaml.load(open('./credentials.yml'), yaml.Loader)
    configs['username'] = credentials['username']
    configs['password'] = credentials['password']
    return configs
