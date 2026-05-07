package service

import (
	"math"

	"modbus-mqtt-consumer/internal/core/domain"
)

const chartTargetPoints = 500

// downsampleLTTB reduces a time-series to the requested threshold while preserving shape.
func downsampleLTTB(points []domain.ChartPoint, threshold int) []domain.ChartPoint {
	n := len(points)
	if threshold <= 0 || n <= threshold || threshold < 3 {
		out := make([]domain.ChartPoint, n)
		copy(out, points)
		return out
	}

	result := make([]domain.ChartPoint, 0, threshold)
	result = append(result, points[0])

	bucketSize := float64(n-2) / float64(threshold-2)
	var a int

	for i := 0; i < threshold-2; i++ {
		avgStart := int(math.Floor(float64(i+1)*bucketSize)) + 1
		avgEnd := int(math.Floor(float64(i+2)*bucketSize)) + 1
		if avgEnd > n {
			avgEnd = n
		}
		if avgStart >= avgEnd {
			avgStart = avgEnd - 1
		}

		avgX, avgY := bucketAverage(points[avgStart:avgEnd])

		rangeStart := int(math.Floor(float64(i)*bucketSize)) + 1
		rangeEnd := int(math.Floor(float64(i+1)*bucketSize)) + 1
		if rangeEnd > n-1 {
			rangeEnd = n - 1
		}

		selected := rangeStart
		maxArea := -1.0
		for j := rangeStart; j < rangeEnd; j++ {
			area := triangleArea(points[a], points[j], avgX, avgY)
			if area > maxArea {
				maxArea = area
				selected = j
			}
		}

		result = append(result, points[selected])
		a = selected
	}

	result = append(result, points[n-1])
	return result
}

func bucketAverage(points []domain.ChartPoint) (float64, float64) {
	if len(points) == 0 {
		return 0, 0
	}

	var sumX float64
	var sumY float64
	for _, p := range points {
		sumX += float64(p.X)
		sumY += p.Y
	}

	count := float64(len(points))
	return sumX / count, sumY / count
}

func triangleArea(a, b domain.ChartPoint, avgX, avgY float64) float64 {
	return math.Abs((float64(a.X)-avgX)*(b.Y-float64(a.Y))-(float64(a.X)-float64(b.X))*(avgY-float64(a.Y))) / 2
}
