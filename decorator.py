from time import time

# Decorator Example


def outer_func(function):
    def inout(*args):
        print('Ahmet')
        function(*args)
    return inout


@outer_func
def another(name):
    print('Ben ' + name)


# Timer Decorator Example

def timer_func(func):
    def logic(*args, **kwargs):
        t1 = time()
        result_func = func(*args, **kwargs)
        t2 = time()
        print(f'This is executed by {func.__name__!r} Log time = {(t2 - t1):.4f}secs')
        return result_func
    return logic


@timer_func
def useful():
    for i in range(100):
        for j in range(1000):
            for k in range(100):
                print('Yagmur the Great')


def main():
    useful()


if __name__ == '__main__':
    main()
