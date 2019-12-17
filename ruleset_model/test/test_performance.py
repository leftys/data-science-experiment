import pandas as pd
import numpy as np

from ruleset_model.performance import check_dataframes, memory_usage



def test_check_dataframes():
	df = pd.DataFrame({
		'test_float1': np.ones(10_000),
		'obj': 'asd',
	})
	# df['obj'] = df['obj'].astype('|S5')
	view = df.obj.loc[:5_000]
	assert view._is_view
	copy = df[:5000]

	check_dataframes(minimal_memory_usage = 0.01, show_views = False)


def test_memory_usage():
	with memory_usage('test') as mem:
		df = pd.DataFrame({
			'test_float1': np.ones(10_000_000),
			'obj': 'asd',
		})

	assert mem[1] > 0


### Copy vs view on dataframe ###

def test_nocopy():
	df = pd.DataFrame({
		'test_float1': np.ones(10_000),
		'test_float2': np.ones(10_000),
	})
	view = df[:5_000]
	assert view._is_view


def test_dtypes_copy():
	''' Combining dtypes causes slicing to perform copy! '''
	df = pd.DataFrame({
		'test_float': np.ones(10_000),
		'test_int': np.ones(10_000).astype('int'),
	})
	view = df[:5_000]
	assert not view._is_view


def test_index_copy():
	''' Slicing using index performs copy! '''
	df = pd.DataFrame({
		'test_float': np.ones(10_000),
		'test_int': np.ones(10_000).astype('int'),
	})
	view = df[df.test_float < 123]
	assert not view._is_view


def test_check_index_copy():
	''' Slicing using index performs copy! '''
	df = pd.DataFrame({
		'test_float': np.ones(10_000),
		'test_int': np.ones(10_000).astype('int'),
	})
	view = df[df.test_float < 123]
	assert not view._is_view


def test_index_nocopy():
	'''  '''
	df = pd.DataFrame({
		'zero_ones': (np.random.random(10_000) + 0.5).astype('int'),
		'row_number': np.ones(10_000).cumsum() - 1,
		'test_float': np.ones(10_000),
	})
	df = df.set_index(['zero_ones', 'row_number'])
	view = df.loc[0]
	print(view)
	assert view._is_view
