单元测试

重写了unittest.TestLoader的_find_tests.支持只加载TestCase


test_project:测试整个项目的所有TestCase,并且会加载所有的模块
test_module:测试整个项目的所有TestCase

TIPS:
在使用fastApi的testClient时,app加了prefix可能会导致找不到路由