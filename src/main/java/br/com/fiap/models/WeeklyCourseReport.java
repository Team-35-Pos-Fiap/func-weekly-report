package br.com.fiap.models;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

public class WeeklyCourseReport {

    private UUID courseId;
    private String courseTitle;

    private UUID teacherId;
    private String teacherName;
    private String teacherEmail;

    private LocalDate startDate;
    private LocalDate endDate;

    private int totalReviews;
    private double averageRating;

    private int urgenteReviewsCount;
    private List<String> urgenteReviewComments;

    public WeeklyCourseReport(UUID courseId, String courseTitle,
                              UUID teacherId, String teacherName, String teacherEmail,
                              LocalDate startDate, LocalDate endDate,
                              int totalReviews, double averageRating,
                              int urgentReviewsCount, List<String> urgentReviewComments) {

        this.courseId = courseId;
        this.courseTitle = courseTitle;
        this.teacherId = teacherId;
        this.teacherName = teacherName;
        this.teacherEmail = teacherEmail;
        this.startDate = startDate;
        this.endDate = endDate;
        this.totalReviews = totalReviews;
        this.averageRating = averageRating;
        this.urgenteReviewsCount = urgentReviewsCount;
        this.urgenteReviewComments = urgentReviewComments;
    }

    public UUID getCourseId() {
        return courseId;
    }

    public void setCourseId(UUID courseId) {
        this.courseId = courseId;
    }

    public String getCourseTitle() {
        return courseTitle;
    }

    public void setCourseTitle(String courseTitle) {
        this.courseTitle = courseTitle;
    }

    public UUID getTeacherId() {
        return teacherId;
    }

    public void setTeacherId(UUID teacherId) {
        this.teacherId = teacherId;
    }

    public String getTeacherName() {
        return teacherName;
    }

    public void setTeacherName(String teacherName) {
        this.teacherName = teacherName;
    }

    public String getTeacherEmail() {
        return teacherEmail;
    }

    public void setTeacherEmail(String teacherEmail) {
        this.teacherEmail = teacherEmail;
    }

    public LocalDate getStartDate() {
        return startDate;
    }

    public void setStartDate(LocalDate startDate) {
        this.startDate = startDate;
    }

    public LocalDate getEndDate() {
        return endDate;
    }

    public void setEndDate(LocalDate endDate) {
        this.endDate = endDate;
    }

    public int getTotalReviews() {
        return totalReviews;
    }

    public void setTotalReviews(int totalReviews) {
        this.totalReviews = totalReviews;
    }

    public double getAverageRating() {
        return averageRating;
    }

    public void setAverageRating(double averageRating) {
        this.averageRating = averageRating;
    }

    public int getUrgentReviewsCount() {
        return urgenteReviewsCount;
    }

    public void setUrgentReviewsCount(int urgentReviewsCount) {
        this.urgenteReviewsCount = urgentReviewsCount;
    }

    public List<String> getUrgentReviewComments() {
        return urgenteReviewComments;
    }

    public void setUrgentReviewComments(List<String> urgentReviewComments) {
        this.urgenteReviewComments = urgentReviewComments;
    }
    
}
