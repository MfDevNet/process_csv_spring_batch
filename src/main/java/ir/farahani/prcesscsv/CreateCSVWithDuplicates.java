package ir.farahani.prcesscsv;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class CreateCSVWithDuplicates {

    public static void main(String[] args) throws IOException {
        final int numLines = 5_00_000;
        final double duplicateProb = 0.2; // 20% chance of duplicate account

        List<String[]> data = new ArrayList<>();
        data.add(new String[]{"nationalId", "account", "salary"}); // Header row

        // Generate random national IDs
        Set<String> existingAccounts = new HashSet<>(); // Track unique accounts
        for (int i = 0; i < numLines; i++) {
            String nationalId = "NID-" + generateRandomNumber(100000, 999999);
            String account;
            if (Math.random() < duplicateProb) {
                // Add a duplicate account with a probability of duplicateProb
                account = getRandomAccount(existingAccounts);
            } else {
                account = "ACC-" + generateRandomNumber(100000, 999999);
                existingAccounts.add(account);
            }
            int salary = generateRandomNumber(10000, 50000);
            data.add(new String[]{nationalId, account, String.valueOf(salary)});
        }

        // Write data to CSV file
        FileWriter writer = new FileWriter("src/main/resources/data1000000.csv");
        for (String[] line : data) {
            StringBuilder sb = new StringBuilder();
            for (String field : line) {
                sb.append(field).append(",");
            }
            sb.deleteCharAt(sb.length() - 1); // Remove trailing comma
            writer.write(sb.toString() + "\n");
        }
        writer.close();

        System.out.println("CSV file created successfully!");
    }

    private static int generateRandomNumber(int min, int max) {
        return new Random().nextInt(max - min + 1) + min;
    }

    private static String getRandomAccount(Set<String> existingAccounts) {
        return (String) existingAccounts.toArray()[new Random().nextInt(existingAccounts.size())];
    }
}
