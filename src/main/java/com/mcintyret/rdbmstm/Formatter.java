package com.mcintyret.rdbmstm;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.mcintyret.rdbmstm.core.Relation;
import com.mcintyret.rdbmstm.core.Value;

public class Formatter {

    public static String toString(Relation relation) {
        String[] headers = relation.getColumnNames().toArray(new String[relation.getColumnNames().size()]);
        int width = headers.length;

        List<String[]> rows = new ArrayList<>();
        rows.add(headers);

        int[] maxWidths = new int[width];
        for (int i = 0; i < width; i++) {
            maxWidths[i] = headers[i].length();
        }

        relation.getValues().forEach(vals -> {
            Iterator<Value> it = vals.iterator();
            String[] row = new String[width];
            for (int i = 0; i < width; i++) {
                row[i] = it.next().toString();

                maxWidths[i] = Math.max(maxWidths[i], row[i].length());
            }
            rows.add(row);
        });

        // add 2 to the maxWidths, to give a minimum of one space padding on either side
        for (int i = 0; i < width; i++) {
            maxWidths[i] += 2;
        }

        StringBuilder sb = new StringBuilder();
        appendRowDelimiter(sb, maxWidths);
        sb.append('\n');
        for (String[] row : rows) {
            appendRow(sb, row, maxWidths);
            sb.append('\n');
            appendRowDelimiter(sb, maxWidths);
            sb.append('\n');
        }

        return sb.toString();
    }

    private static void appendRow(StringBuilder sb, String[] row, int[] maxWidths) {
        sb.append('|');
        for (int i = 0; i < row.length; i++) {
            sb.append(pad(row[i], maxWidths[i])).append('|');
        }

    }

    private static void appendRowDelimiter(StringBuilder sb, int[] maxWidths) {
        sb.append('+');
        for (int maxWidth : maxWidths) {
            for (int j = 0; j < maxWidth; j++) {
                sb.append('-');
            }
            sb.append('+');
        }
    }

    private static String pad(String in, int totalWidth) {
        int paddingEachSide = (totalWidth - in.length()) / 2;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < paddingEachSide; i++) {
            sb.append(' ');
        }
        sb.append(in);
        while (sb.length() < totalWidth) {
            sb.append(' ');
        }

        return sb.toString();
    }

}
