// Code generated by mockery v1.0.0
package kafkactl

import mock "github.com/stretchr/testify/mock"

// MockStrategy is an autogenerated mock type for the Strategy type
type MockStrategy struct {
	mock.Mock
}

// Assignments provides a mock function with given fields: configs
func (_m *MockStrategy) Assignments(configs StrategyConfigs) ([]PartitionDistribution, error) {
	ret := _m.Called(configs)

	var r0 []PartitionDistribution
	if rf, ok := ret.Get(0).(func(StrategyConfigs) []PartitionDistribution); ok {
		r0 = rf(configs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]PartitionDistribution)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(StrategyConfigs) error); ok {
		r1 = rf(configs)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Name provides a mock function with given fields:
func (_m *MockStrategy) Name() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}