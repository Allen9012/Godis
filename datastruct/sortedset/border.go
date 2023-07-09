package sortedset

/**
  Copyright © 2023 github.com/Allen9012 All rights reserved.
  @author: Allen
  @since: 2023/7/8
  @desc:
  @modified by:
**/

/*
 * ScoreBorder is a struct represents `min` `max` parameter of redis command `ZRANGEBYSCORE`
 * can accept:
 *   int or float value, such as 2.718, 2, -2.718, -2 ...
 *   exclusive int or float value, such as (2.718, (2, (-2.718, (-2 ...
 *   infinity: +inf, -inf， inf(same as +inf)
 */

const (
	scoreNegativeInf int8 = -1
	scorePositiveInf int8 = 1
	lexNegativeInf   int8 = '-'
	lexPositiveInf   int8 = '+'
)

type Border interface {
	greater(element *Element) bool
	less(element *Element) bool
	getValue() interface{}
	getExclude() bool
	isIntersected(max Border) bool
}

//
// ScoreBorder represents range of a float value, including: <, <=, >, >=, +inf, -inf
//  @Description:
//  @Implement Border
//
type ScoreBorder struct {
	Inf     int8
	Value   float64
	Exclude bool
}

var scorePositiveInfBorder = &ScoreBorder{
	Inf: scorePositiveInf,
}

var scoreNegativeInfBorder = &ScoreBorder{
	Inf: scoreNegativeInf,
}

//
//
// greater
//  @Description: if max.greater(score) then the score is within the upper border do not use min.greater()
//  @receiver border
//  @param element
//  @return bool
//  @Implement
func (border *ScoreBorder) greater(element *Element) bool {
	value := element.Score
	if border.Inf == scoreNegativeInf {
		return false
	} else if border.Inf == scorePositiveInf {
		return true
	}
	if border.Exclude {
		return border.Value > value
	}
	return border.Value >= value
}

//
// less
//  @Description:
//  @receiver border
//  @param element
//  @return bool
//  @Implement
func (border *ScoreBorder) less(element *Element) bool {
	value := element.Score
	if border.Inf == scoreNegativeInf {
		return true
	} else if border.Inf == scorePositiveInf {
		return false
	}
	if border.Exclude {
		return border.Value < value
	}
	return border.Value <= value
}

//
// getValue
//  @Description:
//  @receiver border
//  @return interface{}
//  @Implement
func (border *ScoreBorder) getValue() interface{} {
	return border.Value
}

//
// getExclude
//  @Description:
//  @receiver border
//  @return bool
//  @Implement
func (border *ScoreBorder) getExclude() bool {
	return border.Exclude
}

//
// isIntersected
//  @Description:  if min is greater than max, or min is equal to max and min is exclusive, then the two borders are not intersected
//  @receiver border
//  @param max
//  @return bool
//  @Implement
func (border *ScoreBorder) isIntersected(max Border) bool {
	minValue := border.Value
	maxValue := max.(*ScoreBorder).Value
	return minValue > maxValue || (minValue == maxValue && (border.getExclude() || max.getExclude()))
}
