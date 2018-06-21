package core.二次排序;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * @Author:HuRan
 * @Description:
 * @Date: Created in 22:01 2018/6/12
 * @Modified By:
 */
public class SecondSortKey implements Ordered<SecondSortKey>, Serializable {
    //首先在自定义key里面
    private int first;
    private int second;

    SecondSortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compare(SecondSortKey that) {
        if (this.first - that.getFirst() != 0) {
            return this.first - that.getFirst();
        } else {
            return this.second - that.getSecond();
        }
    }

    @Override
    public boolean $less(SecondSortKey that) {
        if (this.first < that.getFirst()) {
            return true;
        } else if (this.first == that.getFirst() && this.second > that.getSecond()) {
            return true;
        }
        return false;
    }

    //定义这个key在什么情况下是大于其他key的
    @Override
    public boolean $greater(SecondSortKey that) {
        if (this.first > that.getFirst()) {
            return true;
        } else if (this.first == that.getFirst() && this.second > that.second) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondSortKey that) {
        if (this.$less(that)) {
            return true;
        } else if (this.first == that.getFirst() &&
                this.second == that.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(SecondSortKey that) {
        if (this.$greater(that)) {
            return true;
        } else if (this.first == that.getFirst() && this.second == that.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(SecondSortKey that) {
        if (this.first - that.getFirst() != 0) {
            return this.first - that.getFirst();
        } else {
            return this.second - that.getSecond();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SecondSortKey that = (SecondSortKey) o;

        if (first != that.first) return false;
        return second == that.second;
    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + second;
        return result;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }
}
