package rabbitmq_topic

import "errors"

// errorsIs 独立封装便于测试替换
func errorsIs(err, target error) bool {
	return errors.Is(err, target)
}
