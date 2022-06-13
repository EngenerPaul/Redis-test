from django.conf import settings
from django.views.generic import TemplateView

import redis


redis_instance = redis.StrictRedis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=0,
    password=settings.REDIS_PASSWORD
)


def highlight(func):
    def wrap(*args, **kwargs):
        print(f'THE {func.__name__.upper()} TEST STARTED')
        print('-----------------------------------------')
        func(*args, **kwargs)
        print('-----------------------------------------')
        print(f'THE {func.__name__.upper()} TEST FINISHED')
        print()
    return wrap


class TestCommonFuncMixin():

    @highlight
    def test_common_functions(self):
        """ Tests of all function for simple data types """

        self.redis_set('any_key_1', 'any_value_1')
        self.redis_set('any_key_2', 'any_value_2')
        self.redis_set('any_key_3', 'any_value_3')
        self.redis_keys()

        self.redis_get('any_key_1')
        self.redis_get('non_existent_key')

        self.redis_del('any_key_3')
        self.redis_del('non_existent_key')

        self.redis_set('any_key_2', '10')
        self.redis_incr('any_key_2')
        self.redis_get('any_key_2')

        self.redis_exists('any_key_1')
        self.redis_exists('non_existent_key')

        self.redis_ttl('any_key_1')
        self.redis_ttl('non_existent_key')

        self.redis_getset('any_key_1', 'new_value')
        self.redis_getset('any_key_4',  'new_value')

        self.redis_ttl('any_key_1')
        self.redis_expire('any_key_1', 10)
        self.redis_ttl('any_key_1')
        self.redis_persist('any_key_1')
        self.redis_ttl('any_key_1')
        self.redis_persist('non_existent_key')

    def redis_set(self, name, value, ex=None, px=None):
        redis_instance.set(
            name=name,
            value=value,
            ex=ex,  # seconds
            px=px  # milliseconds
        )

    def redis_keys(self):
        keys = redis_instance.keys()
        keys = [key.decode('utf-8') for key in keys]
        if keys:
            print(f'All keys are: {", ".join(keys)}.')
        else:
            print("Keys list is empty")

    def redis_get(self, name):
        value = redis_instance.get(name=name)
        value = value.decode('utf-8') if value else value
        print(f'Value for key "{name}" is {value}')

    def redis_del(self, name):
        response = redis_instance.delete(name)
        if response:
            print(f'The key {name} has been deleted')
        else:
            print(f"The key {name} doens't exist")

    def redis_incr(self, name, delta=1):
        redis_instance.incr(
            name=name,
            amount=delta
        )

    def redis_fluchall(self):
        redis_instance.flushall()  # always True
        print("All keys have been deleted")

    def redis_exists(self, key):
        response = redis_instance.exists(key)
        if response:
            print(f"The {key} key exists")
        else:
            print(f"The {key} key doesn't exist")

    def redis_ttl(self, key):
        response = redis_instance.ttl(key)
        if response == -1:
            print(f"The ttl of {key} key is endless")
        elif response == -2:
            print(f"The {key} key doesn't exist")
        else:
            print(f"The ttl of {key} key is {response}")  # seconds

    def redis_getset(self, key, value):
        response = redis_instance.getset(
            name=key,
            value=value
        )
        if response:
            print(f"The outdated value of {key} key is "
                  f"{response.decode('utf-8')}")
        else:
            print(f"The new key ({key}) created")

    def redis_persist(self, key):
        response = redis_instance.persist(key)
        # True if key exists

    def redis_expire(self, key, time):
        redis_instance.expire(
            name=key,
            time=time
        )


class TestDictMixin():

    @highlight
    def test_dict(self):
        self.redis_dict_set('set_name_1', 'set_name_1_key_1', 'value_1')
        self.redis_dict_set('set_name_1', 'set_name_1_key_2', 'value_2')
        self.redis_dict_set('set_name_1', 'set_name_1_key_3', 'value_2')
        self.redis_dict_get('set_name_1', 'set_name_1_key_1')
        self.redis_dict_getall('set_name_1')

        self.redis_dict_del('set_name_1', 'set_name_1_key_1',
                            'set_name_1_key_2', 'set_name_1_key_3')
        self.redis_dict_getall('set_name_1')

    def redis_dict_set(self, name, key, value):
        response = redis_instance.hset(
            name=name,
            key=key,
            value=value
        )  # True
        print(f'The {name} hash with {key} key created')

    def redis_dict_get(self, name, key):
        response = redis_instance.hget(
            name=name,
            key=key
        )
        response = response.decode('utf-8')
        print(f"The value of {key} key {name} hash is {response}")

    def redis_dict_getall(self, name):
        response = redis_instance.hgetall(name)
        # type of all elements is byte
        for key in response.keys():
            value = response[key].decode('utf-8')
            print(f"The value of {key.decode('utf-8')} key is {value}. ",
                  end=' ')
        print()

    def redis_dict_del(self, name, *keys):
        response = redis_instance.hdel(name, *keys)
        print(f'Amount of deleted keys is {response}')


class TestSetMixin():

    @highlight
    def test_set(self):
        self.redis_set_set('set_1', 'value_1', 'value_2')
        self.redis_set_set('set_1', 'value_3')
        self.redis_set_get('set_1')
        
        self.redis_set_pop('set_1')
        self.redis_set_get('set_1')

        self.redis_set_set('set_2', 'value_1', 'value_4')
        self.redis_set_get('set_1')
        self.redis_set_get('set_2')
        self.redis_set_union('set_1', 'set_2')
        self.redis_set_diff('set_1', 'set_2')
        self.redis_set_inter('set_1', 'set_2')

    def redis_set_set(self, name, *values):
        response = redis_instance.sadd(name, *values)
        print(f"To {name} set passed {response} values")

    def redis_set_get(self, name):
        response = redis_instance.smembers(name=name)
        response = {value.decode('utf-8') for value in response}
        print(f"The {name} set: {response}")

    def redis_set_pop(self, name):
        response = redis_instance.spop(name)
        print(response.decode('utf-8'))

    def redis_set_union(self, *names):
        response = redis_instance.sunion(*names)
        response = {value.decode('utf-8') for value in response}
        print(f"The union of {', '.join(names[:-1])} is {response}")

    def redis_set_diff(self, *names):
        response = redis_instance.sdiff(*names)
        response = {value.decode('utf-8') for value in response}
        print(f"The differense of {', '.join(names)} is {response}")

    def redis_set_inter(self, *names):
        response = redis_instance.sinter(*names)
        response = {value.decode('utf-8') for value in response}
        print(f"The inter join of {', '.join(names)} is {response}")


class TestListMixin():

    @highlight
    def test_list(self):
        self.redis_list_lpush('list_1', 'value_1', 'value_2')
        self.redis_list_rpush('list_1', 'value_3')

        self.redis_list_lget('list_1')

        self.redis_list_lpop('list_1')
        self.redis_list_lget('list_1')

        self.redis_list_rpop('list_1')
        self.redis_list_lget('list_1')

    def redis_list_lpush(self, name, *values):
        response = redis_instance.lpush(name, *values)
        print(f"Amount added values is {response}")

    def redis_list_rpush(self, name, *values):
        response = redis_instance.rpush(name, *values)
        print(f"Amount added values is {response}")

    def redis_list_lget(self, name, start=0, end=-1):
        response = redis_instance.lrange(name, start, end)
        response = [value.decode('utf-8') for value in response]
        print(f"The {name} list is {response}")

    def redis_list_lpop(self, name):
        response = redis_instance.lpop(name)
        response = response.decode('utf-8')
        print(f"The left value of {name} list was {response}")

    def redis_list_rpop(self, name):
        response = redis_instance.rpop(name)
        response = response.decode('utf-8')
        print(f"The right value of {name} list was {response}")

class HomeView(
            TestCommonFuncMixin,
            TestDictMixin,
            TestSetMixin,
            TestListMixin,
            TemplateView
        ):

    template_name = 'myapp/index.html'

    def get(self, request, *args, **kwargs):
        self.redis_do()
        return super().get(request, *args, **kwargs)

    def redis_do(self):
        self.test_common_functions()
        self.test_dict()
        self.test_set()
        self.test_list()

        self.redis_fluchall()
        self.redis_keys()
