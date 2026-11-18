package cz.polymarket.bot.infra;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Synthetic verification tests for unified Polymarket Bot CloudFormation templates ({@code polymarket-bot-base.yaml},
 * {@code polymarket-bot-ec2.yaml}, {@code telegram-alert-lambda.yaml}) and the {@code deploy-polymarket-bot.sh}
 * health-check &amp; auto-rollback script.
 */
class PolymarketBotInfraVerificationTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldDefine100GbEncryptedRetainedEbsVolumeUnifiedBotServiceAndSingleLogSubscriptionInCloudFormation() throws Exception {
        Path repoRoot = resolveRepoRoot();
        String baseYaml = Files.readString(repoRoot.resolve("infra/polymarket-bot-base.yaml"));
        String ec2Yaml = Files.readString(repoRoot.resolve("infra/polymarket-bot-ec2.yaml"));
        String alertYaml = Files.readString(repoRoot.resolve("infra/telegram-alert-lambda.yaml"));

        assertThat(baseYaml)
                .contains("EbsVolumeSizeGb:")
                .contains("Default: 100")
                .contains("VolumeType: gp3")
                .contains("Encrypted: true")
                .contains("DeletionPolicy: Retain")
                .contains("/polymarket-bot/${AWS::StackName}/bot");

        assertThat(ec2Yaml)
                .contains("Default: polymarket-bot-base")
                .contains("Default: c8gn.xlarge")
                .contains("AllowedValues: [LIVE, PAPER, OFF]")
                .contains("/mnt/ebs/polymarket-bot/collector")
                .contains("/mnt/ebs/polymarket-bot/paper")
                .contains("/mnt/ebs/polymarket-bot/state/runtime-state.json")
                .contains("-Xms4g")
                .contains("-Xmx4g")
                .contains("polymarket-bot.service")
                .contains("/usr/local/bin/deploy-polymarket-bot.sh")
                .contains("http://127.0.0.1:443/api/health")
                .contains("Rolling back to previous working release");

        assertThat(alertYaml)
                .contains("BotBaseStackName:")
                .contains("Default: polymarket-bot-base")
                .contains("SourceAccount: !Ref AWS::AccountId")
                .doesNotContain("LiveBaseStackName")
                .doesNotContain("DataBaseStackName");

        assertThat(Files.exists(repoRoot.resolve("infra/live-trading-base.yaml"))).isFalse();
        assertThat(Files.exists(repoRoot.resolve("infra/live-trading-ec2.yaml"))).isFalse();
        assertThat(Files.exists(repoRoot.resolve("infra/data-collector-base.yaml"))).isFalse();
        assertThat(Files.exists(repoRoot.resolve("infra/data-collector-ec2.yaml"))).isFalse();
        assertThat(Files.exists(repoRoot.resolve("infra/deploy-live.ps1"))).isFalse();
        assertThat(Files.exists(repoRoot.resolve("infra/deploy-data.ps1"))).isFalse();
    }

    @Test
    void shouldVerifyExtractedDeployScriptSucceedsWhenHealthyAndRollsBackToPreviousReleaseWhenHealthCheckFails() throws Exception {
        Path repoRoot = resolveRepoRoot();
        String ec2Yaml = Files.readString(repoRoot.resolve("infra/polymarket-bot-ec2.yaml"));

        Pattern scriptPattern = Pattern.compile(
                "cat << 'EOF' > /usr/local/bin/deploy-polymarket-bot\\.sh\\R(.*?)\\R\\s*EOF",
                Pattern.DOTALL
        );
        Matcher matcher = scriptPattern.matcher(ec2Yaml);
        assertThat(matcher.find()).isTrue();

        // Normalize CloudFormation Sub ${!VAR} escapes to standard Bash ${VAR} and strip indentation
        String rawScript = matcher.group(1)
                .replaceAll("(?m)^ {14}", "")
                .replace("${!", "${")
                .replace("${AWS::Region}", "eu-west-1");

        if (!isBashWithJqAndTarAvailable()) {
            assertThat(rawScript).contains("mv \"$PREVIOUS_DIR\" \"$APP_DIR\"");
            return;
        }

        Path binDir = tempDir.resolve("bin");
        Files.createDirectories(binDir);
        Path deployScript = tempDir.resolve("deploy-polymarket-bot.sh");
        Files.writeString(deployScript, rawScript.replace("\r\n", "\n"), StandardCharsets.UTF_8);

        Path appDir = tempDir.resolve("opt/polymarket-bot");
        Path logFile = tempDir.resolve("bot.log");
        Files.writeString(logFile, "synthetic log line\n", StandardCharsets.UTF_8);

        // Mock systemctl and curl reading active release marker from appDir/version.txt
        Files.writeString(binDir.resolve("systemctl"), """
                #!/bin/bash
                exit 0
                """, StandardCharsets.UTF_8);
        Files.writeString(binDir.resolve("curl"), """
                #!/bin/bash
                VER=$(cat "%s/version.txt" 2>/dev/null || echo "broken")
                if [ "$VER" = "1.0.0" ]; then
                  echo '{"healthy":true,"strategyMode":"LIVE"}'
                  exit 0
                else
                  echo '{"healthy":false,"issues":["broken release"]}'
                  exit 22
                fi
                """.formatted(toBashPath(appDir)), StandardCharsets.UTF_8);
        makeExecutableIfSupported(binDir.resolve("systemctl"));
        makeExecutableIfSupported(binDir.resolve("curl"));
        makeExecutableIfSupported(deployScript);

        Path release100 = buildReleaseTarball("1.0.0");
        Path release101Broken = buildReleaseTarball("1.0.1-broken");

        // 1. Deploy healthy 1.0.0 -> exits 0, APP_DIR contains 1.0.0
        Process p1 = runDeployScript(deployScript, release100, binDir, appDir, logFile);
        assertThat(p1.waitFor(20, TimeUnit.SECONDS)).isTrue();
        assertThat(p1.exitValue()).isZero();
        assertThat(Files.readString(appDir.resolve("version.txt")).trim()).isEqualTo("1.0.0");

        // 2. Deploy broken 1.0.1-broken -> fails health check, exits 1, and rolls APP_DIR back to 1.0.0!
        Process p2 = runDeployScript(deployScript, release101Broken, binDir, appDir, logFile);
        assertThat(p2.waitFor(20, TimeUnit.SECONDS)).isTrue();
        String stderr = new String(p2.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(p2.exitValue()).isEqualTo(1);
        assertThat(stderr).contains("ROLLBACK SUCCESSFUL");
        assertThat(Files.readString(appDir.resolve("version.txt")).trim()).isEqualTo("1.0.0");
        assertThat(Files.readString(tempDir.resolve("opt/polymarket-bot.failed/version.txt")).trim()).isEqualTo("1.0.1-broken");
    }

    private Path buildReleaseTarball(String version) throws Exception {
        Path src = tempDir.resolve("src-" + version);
        Files.createDirectories(src);
        Files.writeString(src.resolve("quarkus-run.jar"), "jar-" + version, StandardCharsets.UTF_8);
        Files.writeString(src.resolve("version.txt"), version + "\n", StandardCharsets.UTF_8);
        Path tarball = tempDir.resolve("bot-" + version + ".tar.gz");
        Process tarProc = new ProcessBuilder(
                "bash", "-c",
                "tar -czf '" + toBashPath(tarball) + "' -C '" + toBashPath(src) + "' ."
        ).start();
        assertThat(tarProc.waitFor(10, TimeUnit.SECONDS)).isTrue();
        assertThat(tarProc.exitValue()).isZero();
        return tarball;
    }

    private Process runDeployScript(Path script, Path archive, Path binDir, Path appDir, Path logFile) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
                "bash",
                toBashPath(script),
                toBashPath(archive)
        );
        String currentPath = pb.environment().getOrDefault("PATH", "");
        pb.environment().put("PATH", toBashPath(binDir) + ":" + currentPath);
        pb.environment().put("POLYMARKET_APP_DIR", toBashPath(appDir));
        pb.environment().put("POLYMARKET_LOG_FILE", toBashPath(logFile));
        pb.environment().put("POLYMARKET_HEALTH_MAX_ATTEMPTS", "2");
        return pb.start();
    }

    private static boolean isBashWithJqAndTarAvailable() {
        try {
            Process p = new ProcessBuilder("bash", "-c", "command -v jq >/dev/null && command -v tar >/dev/null").start();
            return p.waitFor(5, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    private static void makeExecutableIfSupported(Path file) {
        try {
            new ProcessBuilder("bash", "-c", "chmod +x '" + toBashPath(file) + "'").start().waitFor(5, TimeUnit.SECONDS);
        } catch (Exception ignored) {
        }
    }

    private static String toBashPath(Path path) {
        String norm = path.toAbsolutePath().toString().replace('\\', '/');
        if (norm.length() >= 3 && Character.isLetter(norm.charAt(0)) && norm.charAt(1) == ':' && norm.charAt(2) == '/') {
            return "/" + Character.toLowerCase(norm.charAt(0)) + norm.substring(2);
        }
        return norm;
    }

    private static Path resolveRepoRoot() {
        Path cwd = Path.of("").toAbsolutePath();
        if (Files.exists(cwd.resolve("infra/polymarket-bot-base.yaml"))) {
            return cwd;
        }
        Path parent = cwd.getParent();
        if (parent != null && Files.exists(parent.resolve("infra/polymarket-bot-base.yaml"))) {
            return parent;
        }
        throw new IllegalStateException("Could not locate repository root from " + cwd);
    }
}
