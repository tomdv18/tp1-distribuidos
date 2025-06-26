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
    'top_budget': 2,
    'top_rating': 2,
    'top_actors': 2,
    'average_budget': 2,
    'client': 3
}

CLIENT_FILES = [
    {
        'movies': 'movies20.csv',
        'ratings': 'ratings20.csv',
        'credits': 'credits20.csv'
    },
    {
        'movies': 'movies20.csv',
        'ratings': 'ratings20.csv',
        'credits': 'credits20.csv'
    },
    {
        'movies': 'movies21.csv',
        'ratings': 'ratings21.csv',
        'credits': 'credits21.csv'
    }
]
IMMUTABLE_SERVICES = ['rabbitmq', 'gateway', 'model_downloader', 'aggregator_q2', 'aggregator_q3', 'aggregator_q4', 'aggregator_q5']
OUTPUT_FILE = 'docker-compose.yaml'
HEALTH_CHECKERS = 3

def distribute_binds(binds, count):
    base, extra = divmod(len(binds), count)
    result, start = [], 0
    for i in range(count):
        size = base + (1 if i < extra else 0)
        result.append(binds[start:start + size])
        start += size
    return result

def build_base_services():
    base = {
        'rabbitmq': {'build': {'context': './rabbitmq', 'dockerfile': 'rabbitmq.dockerfile'}, 'ports': ['15672:15672'], 'healthcheck': {'test': ['CMD', 'rabbitmqctl', 'status'], 'interval': '10s', 'timeout': '5s', 'retries': 5}},
        'gateway': {'build': {'context': '.', 'dockerfile': 'gateway/gateway.dockerfile'}, 'restart': 'on-failure', 'environment': ['PYTHONUNBUFFERED=1']},
        'model_downloader': {'build': {'context': './model_downloader', 'dockerfile': 'model_downloader.dockerfile'}, 'volumes': ['./model_downloader/model_volume:/models']},
        'aggregator_q2': {'build': {'context': '.', 'dockerfile': 'generic/aggregator_q2/generic.dockerfile'}, 'restart': 'on-failure', 'depends_on': {'rabbitmq': {'condition': 'service_healthy'}}, 'environment': ['PYTHONUNBUFFERED=1', 'BINDS=0,1,2,3,4,5,6,7,8,9', 'CONSUMER_EXCHANGE=top_budget', 'PUBLISHER_EXCHANGE=results', 'NODE_ID=aggregator_q2', 'HEALTH_CHECK_ID=1'], 'volumes': ['./volumes/aggregator_q2:/app/files']},
        'aggregator_q3': {'build': {'context': '.', 'dockerfile': 'generic/aggregator_q3/generic.dockerfile'}, 'restart': 'on-failure', 'depends_on': {'rabbitmq': {'condition': 'service_healthy'}}, 'environment': ['PYTHONUNBUFFERED=1', 'BINDS=0,1,2,3,4,5,6,7,8,9', 'CONSUMER_EXCHANGE=top_rating', 'PUBLISHER_EXCHANGE=results', 'NODE_ID=aggregator_q3', 'HEALTH_CHECK_ID=2'], 'volumes': ['./volumes/aggregator_q3:/app/files']},
        'aggregator_q4': {'build': {'context': '.', 'dockerfile': 'generic/aggregator_q4/generic.dockerfile'}, 'restart': 'on-failure', 'depends_on': {'rabbitmq': {'condition': 'service_healthy'}}, 'environment': ['PYTHONUNBUFFERED=1', 'BINDS=0,1,2,3,4,5,6,7,8,9', 'CONSUMER_EXCHANGE=top_actors', 'PUBLISHER_EXCHANGE=results', 'NODE_ID=aggregator_q4', 'HEALTH_CHECK_ID=3'], 'volumes': ['./volumes/aggregator_q4:/app/files']},
        'aggregator_q5': {'build': {'context': '.', 'dockerfile': 'generic/aggregator_q5/generic.dockerfile'}, 'restart': 'on-failure', 'depends_on': {'rabbitmq': {'condition': 'service_healthy'}}, 'environment': ['PYTHONUNBUFFERED=1', 'BINDS=0,1,2,3,4,5,6,7,8,9', 'CONSUMER_EXCHANGE=average_budget', 'PUBLISHER_EXCHANGE=results', 'NODE_ID=aggregator_q5', f'HEALTH_CHECK_ID={(4 if HEALTH_CHECKERS >= 4 else 1)}'], 'volumes': ['./volumes/aggregator_q5:/app/files']}
    }
    for i in range(1, HEALTH_CHECKERS + 1):
        base[f'health_checker_{i}'] = {
            'build': {'context': '.', 'dockerfile': 'health_checker/health_checker.dockerfile'},
            'restart': 'on-failure',
            'depends_on': {'rabbitmq': {'condition': 'service_healthy'}},
            'environment': [
                'PYTHONUNBUFFERED=1',
                f'PORT=8000',
                f'ID={i}',
                f'HC_SIZE={HEALTH_CHECKERS}'
            ],
            'volumes': ['/var/run/docker.sock:/var/run/docker.sock']
        }
    return base

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
                f"MOVIES_FILE={files['movies']}",
                f"RATINGS_FILE={files['ratings']}",
                f"CREDITS_FILE={files['credits']}"
            ]
        }
    health_check_id_counter = 1
    for key, count in SERVICE_INSTANCES.items():
        if key == 'client':
            continue
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
            env = ['PYTHONUNBUFFERED=1', f"BINDS={','.join(map(str, binds))}", f"NODE_ID={key}_{idx}"]
            if key not in ['gateway', 'model_downloader', 'rabbitmq', 'client']:
                env.append(f"HEALTH_CHECK_ID={health_check_id_counter}")
                health_check_id_counter += 1
                if health_check_id_counter > HEALTH_CHECKERS:
                    health_check_id_counter = 1
            if key == 'top_budget':
                eof_count = SERVICE_INSTANCES.get('filter_one_prod', 0)
                env.append(f"EOF={eof_count}")
            depends = {'rabbitmq': {'condition': 'service_healthy'}}
            if key == 'filter_spain_argentina':
                prev = SERVICE_INSTANCES.get('filter_years_2000_q1', 1)
                for i in range(1, prev+1): depends[f'filter_years_2000_q1_{i}'] = {'condition': 'service_started'}
                env += ['CONSUMER_EXCHANGE=gateway_metadata', 'PUBLISHER_EXCHANGE=filter_spain_argentina']
            if key == 'filter_years_2000_q1':
                env += ['CONSUMER_EXCHANGE=filter_spain_argentina', 'PUBLISHER_EXCHANGE=results']
            if key == 'filter_one_prod':
                prev = SERVICE_INSTANCES.get('top_budget', 1)
                for i in range(1, prev+1): depends[f'top_budget_{i}'] = {'condition': 'service_started'}
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
                prev = SERVICE_INSTANCES.get('top_rating', 1)
                for i in range(1, prev+1): depends[f'top_rating_{i}'] = {'condition': 'service_started'}
                env += ['CONSUMER_EXCHANGE_METADATA=filter_years_2000_q34', 'CONSUMER_EXCHANGE_JOINED=gateway_ratings', 'PUBLISHER_EXCHANGE=join_ratings']
            if key == 'join_credits':
                prev = SERVICE_INSTANCES.get('top_actors', 1)
                for i in range(1, prev+1): depends[f'top_actors_{i}'] = {'condition': 'service_started'}
                env += ['CONSUMER_EXCHANGE_METADATA=filter_years_2000_q34', 'CONSUMER_EXCHANGE_JOINED=gateway_credits', 'PUBLISHER_EXCHANGE=join_credits']
            if key == 'overview':
                prev = SERVICE_INSTANCES.get('average_budget', 1)
                for i in range(1, prev+1): depends[f'average_budget_{i}'] = {'condition': 'service_started'}
                depends['model_downloader'] = {'condition': 'service_completed_successfully'}
                env += ['CONSUMER_EXCHANGE=gateway_metadata', 'PUBLISHER_EXCHANGE=overview']
            services[name] = {'build': {'context': '.', 'dockerfile': dockerfile}, 'restart': 'on-failure', 'depends_on': depends, 'environment': env, 'volumes': [f'./volumes/{name}:/app/files']}
            if key == 'overview':
                services[name].update({'links': ['rabbitmq'], 'volumes': ['./model_downloader/model_volume:/models'], 'healthcheck': {'test': ['CMD', 'test', '-f', '/tmp/model_ready'], 'interval': '5s', 'timeout': '3s', 'retries': 10, 'start_period': '15s'}})
            if key == 'top_budget':
                depends['aggregator_q2'] = {'condition': 'service_started'}
                env += ['CONSUMER_EXCHANGE=filter_one_prod', 'PUBLISHER_EXCHANGE=top_budget']
            if key == 'top_rating':
                depends['aggregator_q3'] = {'condition': 'service_started'}
                env += ['CONSUMER_EXCHANGE=join_ratings', 'PUBLISHER_EXCHANGE=top_rating']
            if key == 'top_actors':
                depends['aggregator_q4'] = {'condition': 'service_started'}
                env += ['CONSUMER_EXCHANGE=join_credits', 'PUBLISHER_EXCHANGE=top_actors']
            if key == 'average_budget':
                depends['aggregator_q5'] = {'condition': 'service_started'}
                env += ['CONSUMER_EXCHANGE=overview', 'PUBLISHER_EXCHANGE=average_budget']

    return services

def assemble_compose():
    compose = {'services': {}}
    compose['services'].update(build_base_services())
    compose['services'].update(generate_scaled_services())
    gw_deps = {}
    for name, svc in compose['services'].items():
        if name not in IMMUTABLE_SERVICES and name != 'gateway' :
            if name.startswith('client_'):
                continue
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
