
import WorkSpawnerConfig


def test_that_concept_thangy(passed_flag):

	print('ok, here is what got passed: ', passed_flag)
	print('here is what is in config file: :', WorkSpawnerConfig.TEST_MODE)
	WorkSpawnerConfig.TEST_MODE = not passed_flag
	print('here is what it is after flipping it: :', WorkSpawnerConfig.TEST_MODE)
	return WorkSpawnerConfig.TEST_MODE

