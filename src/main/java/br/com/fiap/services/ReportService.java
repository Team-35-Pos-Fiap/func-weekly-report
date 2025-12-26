package br.com.fiap.services;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import br.com.fiap.db.PostgresClient;
import br.com.fiap.models.WeeklyCourseReport;

public class ReportService {
    
    private final PostgresClient db;

    public ReportService(PostgresClient db) {
        this.db = db;
    }

    public List<WeeklyCourseReport> generateWeeklyReports() throws SQLException {

        LocalDate end = LocalDate.now();
        LocalDate start = end.minusDays(7);

        List<WeeklyCourseReport> reports = new ArrayList<>();

        String fetchCourses = """
                SELECT 
                    c.id AS course_id,
                    c.title,
                    t.id AS teacher_id,
                    u.first_name,
                    u.last_name,
                    u.email
                FROM courses c
                LEFT JOIN teachers t ON t.id = c.teacher_id
                LEFT JOIN users u ON u.id = t.id
            """;

        try (Connection conn = db.getConnection();
             PreparedStatement stmt = conn.prepareStatement(fetchCourses);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {

                WeeklyCourseReport report = buildCourseReport(
                    conn,
                    rs.getObject("course_id", UUID.class),
                    rs.getString("title"),
                    rs.getObject("teacher_id", UUID.class),
                    rs.getString("first_name") + " " + rs.getString("last_name"),
                    rs.getString("email"),
                    start,
                    end
                );

                reports.add(report);
            }
        }

        return reports;
    }

    private WeeklyCourseReport buildCourseReport(Connection conn,
                                                 UUID courseId,
                                                 String title,
                                                 UUID teacherId,
                                                 String teacherName,
                                                 String teacherEmail,
                                                 LocalDate start,
                                                 LocalDate end) throws SQLException {

//        String sql = """
//            SELECT rating, comment, is_urgent
//            FROM reviews
//            WHERE course_id = ?
//              AND created_at BETWEEN ? AND ?
//        """;

        String sql = """
                SELECT rating, comment, is_urgent
                FROM reviews
                WHERE course_id = ?
            """;

        int totalReviews = 0;
        int ratingSum = 0;

        int urgentCount = 0;
        List<String> urgentComments = new ArrayList<>();

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, courseId);

            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                totalReviews++;
                ratingSum += rs.getInt("rating");

                if (rs.getBoolean("is_urgent")) {
                    urgentCount++;
                    String c = rs.getString("comment");
                    if (c != null && !c.isBlank())
                        urgentComments.add(c);
                }
            }
        }

        double avg = totalReviews == 0 ? 0 : (double) ratingSum / totalReviews;

        return new WeeklyCourseReport(
            courseId, title,
            teacherId, teacherName, teacherEmail,
            start, end,
            totalReviews, avg,
            urgentCount, urgentComments
        );
    }
}
