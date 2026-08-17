package com.gotrustdeal.messaging.architecture;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ArchitectureTest {

    @Test
    void testNoAwsSdkDependenciesInServiceModule() throws IOException {
        Path sourceDir = Paths.get("src/main/java");
        
        // Assert directory exists
        assertTrue(Files.exists(sourceDir), "Source directory must exist");

        List<Path> javaFiles;
        try (Stream<Path> walk = Files.walk(sourceDir)) {
            javaFiles = walk
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".java"))
                    .collect(Collectors.toList());
        }

        assertTrue(!javaFiles.isEmpty(), "There should be Java source files to scan");

        for (Path javaFile : javaFiles) {
            List<String> lines = Files.readAllLines(javaFile);
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i).trim();
                if (line.startsWith("import ")) {
                    // Check against AWS SQS classes
                    boolean hasAwsImport = line.contains("software.amazon.awssdk") ||
                                           line.contains("SqsClient") ||
                                           line.contains("SqsAsyncClient") ||
                                           line.contains("SendMessageRequest") ||
                                           line.contains("ReceiveMessageRequest") ||
                                           line.contains("DeleteMessageRequest") ||
                                           line.contains("GetQueueUrlRequest");
                    
                    assertTrue(!hasAwsImport, String.format(
                            "Architecture violation in %s at line %d: AWS dependency '%s' is not allowed in messaging-service",
                            javaFile.getFileName(), i + 1, line));
                }
            }
        }
    }
}
