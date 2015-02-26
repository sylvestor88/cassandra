package org.apache.cassandra.db.compaction;

import java.util.*;

import org.slf4j.*;

import com.google.common.collect.*;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.*;

/**
 * Created by philipthompson on 2/26/15.
 */
public class BootcampCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(BootcampCompactionStrategy.class);

    protected volatile int estimatedRemainingTasks;
    private final Set<SSTableReader> sstables = new HashSet<>();

    public BootcampCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.estimatedRemainingTasks = 0;
    }

    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        if (!isEnabled())
            return null;

        while (true)
        {
            List<SSTableReader> mostOverlappingBucket = getNextBackgroundSSTables(gcBefore);
            if (mostOverlappingBucket.isEmpty())
                return null;

            if (cfs.getDataTracker().markCompacting(mostOverlappingBucket))
                return new CompactionTask(cfs, mostOverlappingBucket, gcBefore, false);
        }
    }

    private List<SSTableReader> getNextBackgroundSSTables(int gcBefore)
    {
        if (!isEnabled())
            return Collections.emptyList();

        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        Iterable<SSTableReader> candidates = filterSuspectSSTables(Sets.intersection(cfs.getUncompactingSSTables(), sstables));

        List<SSTableReader> mostOverlapped = new ArrayList<SSTableReader>(getMostOverlapping(cfs.getOverlappingSSTables(candidates)));

        if (!mostOverlapped.isEmpty())
            return mostOverlapped;

        // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
        // ratio is greater than threshold.
        List<SSTableReader> sstablesWithTombstones = new ArrayList<>();
        for (SSTableReader sstable : candidates)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                sstablesWithTombstones.add(sstable);
        }
        if (sstablesWithTombstones.isEmpty())
            return Collections.emptyList();

        Collections.sort(sstablesWithTombstones, new SSTableReader.SizeComparator());
        return Collections.singletonList(sstablesWithTombstones.get(0));
    }

    private Set<SSTableReader> getMostOverlapping(Iterable<SSTableReader> candidates)
    {
        ArrayList<SSTableReader> candidateList = new ArrayList<>();
        for (SSTableReader candidate : candidates)
        {
            candidateList.add(candidate);
        }

        Set<SSTableReader> best = null;
        double overlap = 1;
        for (int i = 0; i < candidateList.size(); i++)
        {
            for (int j = 0; j < candidateList.size(); j++)
            {
                if (i != j)
                {
                    Set<SSTableReader> pair = new HashSet<>();
                    pair.add(candidateList.get(i));
                    pair.add(candidateList.get(j));

                    if (SSTableReader.estimateCompactionGain(pair) < overlap)
                    {
                        best = pair;
                        overlap = SSTableReader.estimateCompactionGain(pair);
                    }
                }
            }
        }
        return best;
    }

    public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore)
    {
        Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(sstables);
        if (Iterables.isEmpty(filteredSSTables))
            return null;
        if (!cfs.getDataTracker().markCompacting(filteredSSTables))
            return null;
        return Arrays.<AbstractCompactionTask>asList(new CompactionTask(cfs, filteredSSTables, gcBefore, false));
    }

    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        if (!cfs.getDataTracker().markCompacting(sstables))
        {
            logger.debug("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
            return null;
        }

        return new CompactionTask(cfs, sstables, gcBefore, false).setUserDefined(true);
    }

    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    public void addSSTable(SSTableReader added)
    {
        sstables.add(added);
    }

    public void removeSSTable(SSTableReader sstable)
    {
        sstables.remove(sstable);
    }

}
