package com.otus.db;

import java.sql.*;
import java.util.Random;

public class dz_sql {
    private static final String CONNECTION_URL = "jdbc:mysql://localhost:3306/db";
    private static final String USER = "user1";
    private static final String PASSWORD = "pass";


    private static final String CREATE_CURATOR_SQL = "CREATE TABLE Curator(id int auto_increment primary key, fio varchar(50))";
    private static final String CREATE_GROUP_SQL = "CREATE TABLE StudentsGroup(id int auto_increment primary key, name varchar(50), id_curator int, FOREIGN KEY(id_curator) REFERENCES Curator(id))";
    private static final String CREATE_STUDENT_SQL = "CREATE TABLE Student(id int auto_increment primary key, fio varchar(50), sex varchar(7), id_group int, FOREIGN KEY(id_group) REFERENCES StudentsGroup(id))";

    public void createCuratorTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_CURATOR_SQL);
        }
    }

    public void createStudentsGroupTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_GROUP_SQL);
        }
    }

    public void createStudentTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_STUDENT_SQL);
        }
    }

    private static final String INSERT_INTO_CURATOR = "INSERT INTO Curator(fio) VALUES(?)";
    private static final String INSERT_INTO_GROUP = "INSERT INTO StudentsGroup(name, id_curator) VALUES(?, ?)";
    private static final String INSERT_INTO_STUDENT = "INSERT INTO Student(fio, sex, id_group) VALUES(?, ?, ?)";
    private static final String GET_GROUP_WITH_CURATOR = "SELECT g.id, name, fio FROM StudentsGroup as g JOIN Curator as c ON g.id_curator=c.id";
    private static final String GET_STUDENTS_INFO = "SELECT s.id, s.fio, name, c.fio FROM Student as s JOIN StudentsGroup as g ON s.id_group=g.id JOIN Curator as c ON g.id_curator=c.id";
    private static final String GET_MALE_STUDENTS = "SELECT id, fio, sex FROM Student WHERE sex='M'";
    private static final String GET_FEMALE_STUDENTS = "SELECT id, fio, sex FROM Student WHERE sex='F'";
    private static final String GET_STUDENTS_FROM_GROUP = "SELECT s.id, fio, name FROM Student as s JOIN StudentsGroup as g ON s.id_group=g.id WHERE g.name='Группа1'";
    private static final String UPDATE_GROUP_BY_CURATOR = "UPDATE StudentsGroup SET id_curator=3 WHERE name='Группа1'";

    public void insertDataIntoCuratorTable(Connection connection, String curatorFio) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_INTO_CURATOR)) {
            statement.setString(1, curatorFio);
            statement.executeUpdate();
        }
    }

    public void insertDataIntoStudentsGroupTable(Connection connection, String groupName, int curId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_INTO_GROUP)) {
            statement.setString(1, groupName);
            statement.setInt(2, curId);
            statement.executeUpdate();
        }
    }

    public void insertDataIntoStudentTable(Connection connection, String studentFio, String studentSex, int grId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_INTO_STUDENT)) {
            statement.setString(1, studentFio);
            statement.setString(2, studentSex);
            statement.setInt(3, grId);
            statement.executeUpdate();
        }
    }

    public static void main(String[] args) throws SQLException {
        dz_sql dz_sql = new dz_sql();

        try (Connection connection = DriverManager.getConnection(CONNECTION_URL, USER, PASSWORD)) {
            dz_sql.createCuratorTable(connection);
            dz_sql.createStudentsGroupTable(connection);
            dz_sql.createStudentTable(connection);

            for (int i = 1; i < 5; i++) {
                dz_sql.insertDataIntoCuratorTable(connection, "Куратор" + i);
            }
            for (int i = 1; i < 4; i++) {
                dz_sql.insertDataIntoStudentsGroupTable(connection, "Группа" + i, i);
            }
            for (int i = 1; i < 16; i++) {
                if (i < 8) {
                    dz_sql.insertDataIntoStudentTable(connection, "Student" + i, dz_sql.getRandomSex(), 1);
                } else {
                    dz_sql.insertDataIntoStudentTable(connection, "Student" + i, dz_sql.getRandomSex(), 2);
                }
            }
            System.out.println("5. Вывести на экран всех студентов с названием группы и именем куратора");
            printStudentsInfo(connection);
            System.out.println("6. Вывести на экран количество студентов");
            printMaleStudentsCount(connection);
            System.out.println("7. Вывести студенток");
            printFemaleStudents(connection);
            System.out.println("8. Обновить данные по группе сменив куратора");
            updateGroupByCurator(connection);
            System.out.println("9. Вывести список групп с их кураторами");
            printGroupWithCurator(connection);
            System.out.println("10. Вывести на экран студентов из определенной группы");
            printStudentsFromGroup(connection);
        }
    }

    public static void printStudentsInfo(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(GET_STUDENTS_INFO)) {
            while (resultSet.next()) {
                int idStudent = resultSet.getInt(1);
                String fioStudent = resultSet.getString("fio");
                String groupName = resultSet.getString(3);
                String fioCurator = resultSet.getString(4);

                String row = String.format("ID: %s, FIO: %s, NAME: %s, CURATOR: %s", idStudent, fioStudent, groupName, fioCurator);
                System.out.println(row);
            }
        }
    }

    public static void printMaleStudentsCount(Connection connection) throws SQLException {
        int count = 0;
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(GET_MALE_STUDENTS)) {
            while (resultSet.next()) {
                count = count + 1;
            }
        }
        System.out.println("Количество студентов = " + count);
    }

    public static void printFemaleStudents(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(GET_FEMALE_STUDENTS)) {
            while (resultSet.next()) {
                int idStudent = resultSet.getInt(1);
                String fio = resultSet.getString("fio");
                String sex = resultSet.getString(3);

                String row = String.format("ID: %s, FIO: %s, SEX: %s", idStudent, fio, sex);
                System.out.println(row);
            }
        }
    }

    public static void updateGroupByCurator(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(UPDATE_GROUP_BY_CURATOR);
        }
        printGroupWithCurator(connection);
    }

    public static void printGroupWithCurator(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(GET_GROUP_WITH_CURATOR)) {
            while (resultSet.next()) {
                int idGroup = resultSet.getInt(1);
                String name = resultSet.getString("name");
                String fio = resultSet.getString(3);

                String row = String.format("ID: %s, NAME: %s, FIO: %s", idGroup, name, fio);
                System.out.println(row);
            }
        }
    }

    public static void printStudentsFromGroup(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(GET_STUDENTS_FROM_GROUP)) {
            while (resultSet.next()) {
                int idStudent = resultSet.getInt(1);
                String fio = resultSet.getString("fio");
                String grName = resultSet.getString(3);

                String row = String.format("ID: %s, FIO: %s, NAME: %s", idStudent, fio, grName);
                System.out.println(row);
            }
        }
    }

    public String getRandomSex() {
        Random random = new Random();
        boolean isMale = random.nextBoolean();
        if (isMale) {
            return "M";
        } else {
            return "F";
        }
    }

}
