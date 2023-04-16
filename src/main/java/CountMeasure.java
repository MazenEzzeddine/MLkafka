


public class CountMeasure {
    double duration;
    public double getCount() {
        return duration;
    }
    public void setCount(double duration) {
        this.duration = duration;
    }
    public CountMeasure(double duration) {
        this.duration = duration;
    }

    @Override
    public String toString() {
        return "TimeMeasure{" +
                "duration=" + duration +
                '}';
    }
}