package cz.polymarket.bot.backtest;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@QuarkusTest
class BacktestApplicationTest {

    @Inject
    BacktestApplication application;

    @Test
    @DisplayName("BacktestApplication CDI bean should be injected successfully")
    void shouldInjectBacktestApplication() {
        assertThat(application).isNotNull();
    }

    @Test
    @DisplayName("onStart method should handle StartupEvent without throwing exceptions")
    void shouldHandleStartupEvent() {
        assertThatCode(() -> application.onStart(new StartupEvent()))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("BacktestApplication should have a public static main method")
    void shouldHaveMainMethod() throws NoSuchMethodException {
        Method mainMethod = BacktestApplication.class.getMethod("main", String[].class);
        assertThat(mainMethod).isNotNull();
        assertThat(java.lang.reflect.Modifier.isStatic(mainMethod.getModifiers())).isTrue();
        assertThat(java.lang.reflect.Modifier.isPublic(mainMethod.getModifiers())).isTrue();
    }
}
