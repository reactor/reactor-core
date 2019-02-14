package reactor;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClassList;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchUnitRunner;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.runner.RunWith;
import reactor.core.scheduler.Scheduler;

import static com.tngtech.archunit.lang.conditions.ArchConditions.*;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

@RunWith(ArchUnitRunner.class)
@AnalyzeClasses(packages = "reactor.core")
public class ArchitectureTest {

	@ArchTest
	static final ArchRule shouldNotScheduleRunnables = classes()
			.that()
			.haveNameNotMatching(".*Test.*")
			.and()
			.dontHaveFullyQualifiedName("reactor.core.scheduler.Schedulers$CachedScheduler")
			.and()
			.dontHaveFullyQualifiedName("reactor.core.scheduler.SingleWorkerScheduler")
			.should(never(callMethodWhere(new ScheduleMethodCallPredicate(Scheduler.class))))
			.andShould(never(callMethodWhere(new ScheduleMethodCallPredicate(Scheduler.Worker.class))));

	private static class ScheduleMethodCallPredicate extends DescribedPredicate<JavaMethodCall> {

		final Class<?> targetClass;

		public ScheduleMethodCallPredicate(Class<?> targetClass) {
			super("First argument of " + targetClass.getName() + "#schedule is Runnable");
			this.targetClass = targetClass;
		}

		@Override
		public boolean apply(JavaMethodCall input) {
			if (!input.getTargetOwner().isEquivalentTo(targetClass)) {
				return false;
			}

			JavaClassList parameters = input.getTarget().getParameters();
			if (parameters.isEmpty()) {
				return false;
			}
			return parameters.get(0).isEquivalentTo(Runnable.class);
		}
	}
}
