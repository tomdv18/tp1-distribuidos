import yaml

TOTAL_BINDS = list(range(10))
SERVICE_INSTANCES = {
    'filter_spain_argentina': 2,
    'filter_years_2000_q1': 2,
    'filter_one_prod': 2,
    'filter_argentina': 2,
    'filter_years_2000_q34': 2,
    'join_ratings': 2,
    'join_credits': 2,
    'overview': 2,
    'client': 2
}

CLIENT_FILES = [
    {
        'movies': 'movies_metadata20%.csv',
        'ratings': 'ratings20%.csv',
        'credits': 'credits20%.csv'
    },
    {
        'movies': 'movies_metadata100.csv',
        'ratings': 'ratings50.csv',
        'credits': 'credits100.csv'
    }
]
IMMUTABLE_SERVICES = ['rabbitmq', 'gateway', 'model_downloader', 'average_budget', 'top_budget', 'top_rating', 'top_actors']
OUTPUT_FILE = 'docker-compose.yaml'

def distribute_binds(binds, count):
    base, extra = divmod(len(binds), count)
    result, start = [], 0
    for i in range(count):
        size = base + (1 if i < extra else 0)
        result.append(binds[start:start + size])
        start += size
    return result

def build_base_services():
    client_count = SERVICE_INSTANCES.get('client', 0)

    return {
        'rabbitmq': {'build': {'context': './rabbitmq', 'dockerfile': 'rabbitmq.dockerfile'}, 'ports': ['15672:15672'], 'healthcheck': {'test': ['CMD', 'rabbitmqctl', 'status'], 'interval': '10s', 'timeout': '5s', 'retries': 5}},
        'gateway': {'build': {'context': '.', 'dockerfile': 'gateway/gateway.dockerfile'}, 'restart': 'on-failure', 'environment': ['PYTHONUNBUFFERED=1']},
        'model_downloader': {'build': {'context': './model_downloader', 'dockerfile': 'model_downloader.dockerfile'}, 'volumes': ['./model_downloader/model_volume:/models']},
        'average_budget': {'build': {'context': '.', 'dockerfile': 'generic/average_budget/generic.dockerfile'}, 'restart': 'on-failure', 'depends_on': {'rabbitmq': {'condition': 'service_healthy'}}, 'environment': ['PYTHONUNBUFFERED=1', 'BINDS=0,1,2,3,4,5,6,7,8,9', 'CONSUMER_EXCHANGE=overview', 'PUBLISHER_EXCHANGE=results', 'NODE_ID=average_budget',f'CLIENT_COUNT={client_count}']},
        'top_budget': {'build': {'context': '.', 'dockerfile': 'generic/top_budget/generic.dockerfile'}, 'restart': 'on-failure', 'depends_on': {'rabbitmq': {'condition': 'service_healthy'}}, 'environment': ['PYTHONUNBUFFERED=1', 'BINDS=0,1,2,3,4,5,6,7,8,9', 'CONSUMER_EXCHANGE=filter_one_prod', 'PUBLISHER_EXCHANGE=results', 'NODE_ID=top_budget',f'CLIENT_COUNT={client_count}']},
        'top_rating': {'build': {'context': '.', 'dockerfile': 'generic/top_rating/generic.dockerfile'}, 'restart': 'on-failure', 'depends_on': {'rabbitmq': {'condition': 'service_healthy'}}, 'environment': ['PYTHONUNBUFFERED=1', 'BINDS=0,1,2,3,4,5,6,7,8,9', 'CONSUMER_EXCHANGE=join_ratings', 'PUBLISHER_EXCHANGE=results', 'NODE_ID=top_rating',f'CLIENT_COUNT={client_count}']},
        'top_actors': {'build': {'context': '.', 'dockerfile': 'generic/top_actors/generic.dockerfile'}, 'restart': 'on-failure', 'depends_on': {'rabbitmq': {'condition': 'service_healthy'}}, 'environment': ['PYTHONUNBUFFERED=1', 'BINDS=0,1,2,3,4,5,6,7,8,9', 'CONSUMER_EXCHANGE=join_credits', 'PUBLISHER_EXCHANGE=results', 'NODE_ID=top_actors',f'CLIENT_COUNT={client_count}']},
    }

def generate_scaled_services():
    services = {}
    client_count = SERVICE_INSTANCES.get('client', 0)
    for idx in range(1, client_count + 1):
        name = f"client_{idx}"
        files = CLIENT_FILES[idx - 1] if idx - 1 < len(CLIENT_FILES) else {
            'movies': 'movies_metadata.csv',
            'ratings': 'ratings.csv',
            'credits': 'credits.csv'
        }
        services[name] = {
            'build': {'context': '.', 'dockerfile': 'client/client.dockerfile'},
            'depends_on': {'gateway': {'condition': 'service_started'}},
            'volumes': ['./client/files:/app/files'],
            'environment': [
                'PYTHONUNBUFFERED=1',
                f"ARCHIVO_MOVIES={files['movies']}",
                f"ARCHIVO_RATINGS={files['ratings']}",
                f"ARCHIVO_CREDITS={files['credits']}"
            ]
        }
    for key, count in SERVICE_INSTANCES.items():
        if key == 'client':
            continue  # Ya procesado arriba
        binds_groups = distribute_binds(TOTAL_BINDS, count)
        for idx, binds in enumerate(binds_groups, start=1):
            name = f"{key}_{idx}"
            if key.startswith('filter_'):
                dockerfile = f"filter/{key}/filter.dockerfile"
            elif key.startswith('join_'):
                dockerfile = f"join/{key}/join.dockerfile"
            elif key == 'overview':
                dockerfile = 'generic/overview_processor/generic.dockerfile'
            else:
                dockerfile = f"generic/{key}/generic.dockerfile"
            env = ['PYTHONUNBUFFERED=1', f"BINDS={','.join(map(str, binds))}", f"NODE_ID={key}_{idx}", f"CLIENT_COUNT={SERVICE_INSTANCES.get('client', 0)}"]
            depends = {'rabbitmq': {'condition': 'service_healthy'}}
            if key == 'filter_spain_argentina':
                prev = SERVICE_INSTANCES.get('filter_years_2000_q1', 1)
                for i in range(1, prev+1): depends[f'filter_years_2000_q1_{i}'] = {'condition': 'service_started'}
                env += ['CONSUMER_EXCHANGE=gateway_metadata', 'PUBLISHER_EXCHANGE=filter_spain_argentina']
            if key == 'filter_years_2000_q1':
                env += ['CONSUMER_EXCHANGE=filter_spain_argentina', 'PUBLISHER_EXCHANGE=results']
            if key == 'filter_one_prod':
                depends['top_budget'] = {'condition': 'service_started'}
                env += ['CONSUMER_EXCHANGE=gateway_metadata', 'PUBLISHER_EXCHANGE=filter_one_prod']
            if key == 'filter_argentina':
                prev = SERVICE_INSTANCES.get('filter_years_2000_q34', 1)
                for i in range(1, prev+1): depends[f'filter_years_2000_q34_{i}'] = {'condition': 'service_started'}
                env += ['CONSUMER_EXCHANGE=gateway_metadata', 'PUBLISHER_EXCHANGE=filter_argentina']
            if key == 'filter_years_2000_q34':
                for join_key in ('join_ratings', 'join_credits'):
                    if join_key in IMMUTABLE_SERVICES:
                        depends[join_key] = {'condition': 'service_started'}
                    else:
                        prev = SERVICE_INSTANCES.get(join_key, 1)
                        for i in range(1, prev+1): depends[f'{join_key}_{i}'] = {'condition': 'service_started'}
                env += ['CONSUMER_EXCHANGE=filter_argentina', 'PUBLISHER_EXCHANGE=filter_years_2000_q34']
            if key == 'join_ratings':
                depends['top_rating'] = {'condition': 'service_started'}
                env += ['CONSUMER_EXCHANGE_METADATA=filter_years_2000_q34', 'CONSUMER_EXCHANGE_JOINED=gateway_ratings', 'PUBLISHER_EXCHANGE=join_ratings']
            if key == 'join_credits':
                depends['top_actors'] = {'condition': 'service_started'}
                env += ['CONSUMER_EXCHANGE_METADATA=filter_years_2000_q34', 'CONSUMER_EXCHANGE_JOINED=gateway_credits', 'PUBLISHER_EXCHANGE=join_credits']
            if key == 'overview':
                depends['average_budget'] = {'condition': 'service_started'}
                depends['model_downloader'] = {'condition': 'service_completed_successfully'}
                env += ['CONSUMER_EXCHANGE=gateway_metadata', 'PUBLISHER_EXCHANGE=overview']
            services[name] = {'build': {'context': '.', 'dockerfile': dockerfile}, 'restart': 'on-failure', 'depends_on': depends, 'environment': env}
            if key == 'overview':
                services[name].update({'links': ['rabbitmq'], 'volumes': ['./model_downloader/model_volume:/models'], 'healthcheck': {'test': ['CMD', 'test', '-f', '/tmp/model_ready'], 'interval': '5s', 'timeout': '3s', 'retries': 10, 'start_period': '15s'}})
    return services

def assemble_compose():
    compose = {'services': {}}
    compose['services'].update(build_base_services())
    compose['services'].update(generate_scaled_services())
    gw_deps = {}
    for name, svc in compose['services'].items():
        if name not in IMMUTABLE_SERVICES and name != 'gateway' :
            if name.startswith('client_'):
                continue  # Excluir instancias de client
            if any(e.startswith('PUBLISHER_EXCHANGE=results') for e in svc.get('environment', [])):
                continue
            gw_deps[name] = {'condition': 'service_started'}
    for name in compose['services']:
        if name.startswith('overview_'):
            gw_deps[name] = {'condition': 'service_healthy'}
    gw_deps['rabbitmq'] = {'condition': 'service_healthy'}
    compose['services']['gateway']['depends_on'] = gw_deps
    total_results = sum(1 for s in compose['services'].values() if any(e.startswith('PUBLISHER_EXCHANGE=results') for e in s.get('environment', [])))
    compose['services']['gateway']['environment'].append(f"EOF={total_results}")
    return compose

if __name__ == '__main__':
    with open(OUTPUT_FILE, 'w') as f:
        yaml.safe_dump(assemble_compose(), f, sort_keys=False)
    print(f"Generated {OUTPUT_FILE}")
