/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

public class HudiPushTableVersionIntoTableScan
        implements Rule<TableScanNode>
{
    public static final String HOODIE_CATALOG_NAME = "hudi";
    public static final String HOODIE_COMMIT_TIME = "_hoodie_commit_time";
    private static final Pattern<TableScanNode> PATTERN = tableScan().matching(tableScan -> tableScan.getTable().getCatalogHandle().getId().equals(HOODIE_CATALOG_NAME));

    private final PlannerContext plannerContext;

    private final DomainTranslator domainTranslator;

    public HudiPushTableVersionIntoTableScan(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.domainTranslator = new DomainTranslator(plannerContext);
    }

    @Override
    public Pattern<TableScanNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return Rule.super.isEnabled(session);
    }

    @Override
    public Result apply(TableScanNode node, Captures captures, Context context)
    {
        Optional<ConstraintApplicationResult<TableHandle>> result = plannerContext.getMetadata().applyFilter(
                context.getSession(),
                node.getTable(),
                new Constraint(node.getEnforcedConstraint()));

        if (result.isEmpty()) {
            return Result.empty();
        }
        TableHandle newTable = result.get().getHandle();

        TupleDomain<ColumnHandle> remainingFilter = result.get().getRemainingFilter();
        Map<String, ColumnHandle> columnNamesByHandle = plannerContext.getMetadata().getColumnHandles(context.getSession(), newTable);
        Optional<ColumnHandle> columnOptional = Optional.of(columnNamesByHandle.get(HOODIE_COMMIT_TIME));
        if (columnOptional.isPresent() && remainingFilter.getDomains().isPresent()) {
            Symbol hoodieCommitTime = new Symbol(HOODIE_COMMIT_TIME);
            TupleDomain<Symbol> remainingPredicate = TupleDomain.withColumnDomains(
                    ImmutableMap.of(
                            hoodieCommitTime,
                            remainingFilter.getDomains().get().get(columnOptional.get())));
            Expression remainingExpression = domainTranslator.toPredicate(context.getSession(), remainingPredicate);
            return Result.ofPlanNode(new FilterNode(context.getIdAllocator().getNextId(), node, remainingExpression));
        }
        return Result.empty();
    }

    public static TupleDomain<ColumnHandle> computeEnforced(TupleDomain<ColumnHandle> predicate, TupleDomain<ColumnHandle> unenforced)
    {
        // The engine requested the connector to apply a filter with a non-none TupleDomain.
        // A TupleDomain is effectively a list of column-Domain pairs.
        // The connector is expected enforce the respective domain entirely on none, some, or all of the columns.
        // 1. When the connector could enforce none of the domains, the unenforced would be equal to predicate;
        // 2. When the connector could enforce some of the domains, the unenforced would contain a subset of the column-Domain pairs;
        // 3. When the connector could enforce all of the domains, the unenforced would be TupleDomain.all().

        // In all 3 cases shown above, the unenforced is not TupleDomain.none().
        checkArgument(!unenforced.isNone(), "Unexpected unenforced none tuple domain");

        Map<ColumnHandle, Domain> predicateDomains = predicate.getDomains().get();
        Map<ColumnHandle, Domain> unenforcedDomains = unenforced.getDomains().get();
        ImmutableMap.Builder<ColumnHandle, Domain> enforcedDomainsBuilder = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Domain> entry : predicateDomains.entrySet()) {
            ColumnHandle predicateColumnHandle = entry.getKey();
            Domain predicateDomain = entry.getValue();
            if (unenforcedDomains.containsKey(predicateColumnHandle)) {
                Domain unenforcedDomain = unenforcedDomains.get(predicateColumnHandle);
                checkArgument(
                        predicateDomain.contains(unenforcedDomain),
                        "Unexpected unenforced domain %s on column %s. Expected all, none, or a domain equal to or narrower than %s",
                        unenforcedDomain,
                        predicateColumnHandle,
                        predicateDomain);
            }
            else {
                enforcedDomainsBuilder.put(predicateColumnHandle, predicateDomain);
            }
        }
        return TupleDomain.withColumnDomains(enforcedDomainsBuilder.buildOrThrow());
    }
}
