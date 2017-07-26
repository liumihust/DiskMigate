package org.apache.hadoop.hdfs.server.diskmigration.planner;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolumeSet;
import org.apache.hadoop.hdfs.server.diskmigration.DiskMigrateException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lm140765 on 2017/7/22.
 */
public class MultiDiskMigration {

    /**
     * calculates a migration plan for the volume set.
     *
     * @param volumeSet  - Disk Set,contains old disks and new disks
     * @return Map  - Record the bytes to migrate between each volume pair.
     */
    static Map<String, Map<String,Long>> calculate(DiskBalancerVolumeSet volumeSet)
    throws DiskMigrateException {
        Preconditions.checkNotNull(volumeSet);
        DiskBalancerVolumeSet currentSet = new DiskBalancerVolumeSet(volumeSet);
        currentSet.computeVolumeDataDensity();

        List<DiskBalancerVolume> newDisks = new ArrayList<DiskBalancerVolume>();
        List<DiskBalancerVolume> oldDisks = new ArrayList<DiskBalancerVolume>();
        List<Double> recieveRtio = new ArrayList<Double>();
        Map<String, Map<String,Long>> old2new = new HashMap<String, Map<String,Long>>();
        Long newConfigCapacity = 0L;
        for(DiskBalancerVolume disk : currentSet.getVolumes()){
            if(disk.getUsedRatio() <= 0.01){
                newDisks.add(disk);
                newConfigCapacity += disk.computeEffectiveCapacity();
            }else{
                oldDisks.add(disk);
            }
        }

        for(DiskBalancerVolume disk : newDisks){
            recieveRtio.add(1.0 * disk.computeEffectiveCapacity()/newConfigCapacity);
        }

        for(DiskBalancerVolume disk : oldDisks){
            long bytesToMigrate = disk.getUsed();
            long restBytes = bytesToMigrate;
            for(int i = 0; i< newDisks.size(); i++){
                long receive;
                if(i == newDisks.size()-1){
                    receive = restBytes;
                }else{
                    receive = (long)(recieveRtio.get(i)*bytesToMigrate);
                }

                if(receive > newDisks.get(i).getFreeSpace()){
                    String errormsg = String.format("new disk: %s capacity %d bytes is not enough",
                            newDisks.get(i).getPath(), newDisks.get(i).getCapacity());
                    throw new DiskMigrateException(errormsg,
                            DiskMigrateException.Result.INVALID_NEW_VOLUME);
                }
                if(!old2new.containsKey(disk.getUuid())){
                    old2new.put(disk.getUuid(),new HashMap<String,Long>());
                }
                old2new.get(disk.getUuid()).put(newDisks.get(i).getUuid(),receive);
                restBytes -= receive;
            }
            currentSet.computeVolumeDataDensity();//update the data info of the disks
        }
        return old2new;
    }

}
