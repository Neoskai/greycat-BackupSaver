/**
 * Copyright 2017 The GreyCat Authors.  All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package greycat.backup.tools;

import com.spotify.sparkey.Sparkey;
import com.spotify.sparkey.SparkeyWriter;
import greycat.BackupOptions;
import greycat.Callback;
import greycat.struct.Buffer;
import greycat.struct.BufferIterator;
import io.minio.MinioClient;

import java.io.File;

/**
 * @ignore ts
 */
public class SparkeyBackupStorage {

    private static final String _connectedError = "PLEASE CONNECT YOUR DATABASE FIRST";

    private int _currentFile;
    private int _currentEntries;
    private int _currentPool;

    private String _filePath;
    private boolean _isConnected = false;

    private long _currentTimelapse;

    private SparkeyWriter _writer = null;

    private int _timelapseDuration;
    private int _maxEntry;

    public SparkeyBackupStorage(int poolId){
        _currentTimelapse = 0;
        _currentFile = 0;
        _currentEntries = 0;
        _currentPool = poolId;

        _timelapseDuration = BackupOptions.timelapseDuration();
        _maxEntry = BackupOptions.maxEntry();

        loadFilePath();
    }

    /**
     * Connects the writer for this storage, or creates it if it didn't exist.
     * @param callback Callback function
     */
    public void connect(Callback<Boolean> callback) {
        File indexFile = new File(_filePath);

        try {
            if (!indexFile.exists()) {
                indexFile.mkdirs();
                System.out.println("Creating new file: " + indexFile.getName());
                _writer = Sparkey.createNew(indexFile);
                _writer.flush();
                _writer.writeHash();
                _writer.close();
            }

            _writer = Sparkey.append(indexFile);

            _isConnected = true;
            if (callback != null) {
                callback.on(true);
            }

        } catch (Exception e) {
            e.printStackTrace();
            if (callback != null) {
                callback.on(false);
            }
        }
    }

    /**
     * Processes the buffer and adds the key / value it contains to the database
     * @param stream The buffer to process
     * @param callback Callback function
     */
    public void put(Buffer stream, Callback<Boolean> callback){
        if (!_isConnected) {
            throw new RuntimeException(_connectedError);
        }
        try {
            // Checking if we raised the max number of element in this file or if we need to change cause of timestamp
            swapCheck();
            _currentEntries++;

            BufferIterator it = stream.iterator();
            while (it.hasNext()) {
                Buffer keyView = it.next();
                Buffer valueView = it.next();

                StorageKeyChunk key = StorageKeyChunk.build(keyView);

                //StorageValueChunk value = StorageValueChunk.build(valueView);
                //System.out.println("Received key is : " + key.toString() + " with data: " + value.toString() + " written in " + _filePath);

                if (valueView != null) {
                    // When saving key to base64 format
                    // _writer.process(keyView.data(), valueView.data());

                    // When saving key to string format with ; separator
                    _writer.put(key.buildString().getBytes(), valueView.data());
                }
            }

            swapCheck();

            if (callback != null) {
                callback.on(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (callback != null) {
                callback.on(false);
            }
        }
    }

    /**
     * Check if we need to swap files at the end of an insert (depending on the max number of entry in a file)
     */
    private void swapCheck(){
        long endLapse = (_currentTimelapse+1) * _timelapseDuration;
        if(_currentEntries == _maxEntry || System.currentTimeMillis() > endLapse){
            disconnect(new Callback<Boolean>() {
                @Override
                public void on(Boolean result) {
                    // NOTHING
                }
            });

            loadFilePath();

            connect(new Callback<Boolean>() {
                @Override
                public void on(Boolean result) {
                    // NOTHING
                }
            });

            _currentEntries = 0;
        }

    }

    /**
     * Loads the new filepath
     */
    private void loadFilePath(){
        // Constant part
        String shard = "/shard_" + _currentPool;

        // We then need to change the timelapse after each Backup Point. currentTimelapse is the current UNIT of Timelapseduration.
        _currentTimelapse =  Math.floorDiv(System.currentTimeMillis() , _timelapseDuration);
        String timelapse = "/timelapse_" +  _currentTimelapse + "_" + (_currentTimelapse+1);

        // Finally, we need to swap each file after X Entries, and write the beginning and end timelapse + the last node in this file
        long timeStamp = System.currentTimeMillis();
        String fileId = "/save_" + _currentFile++ + "-" + timeStamp;

        _filePath = "data/logs" + shard + timelapse + fileId + "-.spl";
    }

    /**
     * Disconnects the current storage writer and flush everything to the disk
     * @param callback Callback function
     */
    public void disconnect(Callback<Boolean> callback){
        try{
            _writer.writeHash();
            _writer.flush();
            _writer.close();

            File indexFile = new File(_filePath);
            String[] split = _filePath.split(".spl");
            String newPath = split[0] + System.currentTimeMillis();

            File spiFILE = new File(split[0] + ".spi");

            File newSPIFile = new File(newPath + ".spi");
            File newFile = new File(newPath +".spl");
            indexFile.renameTo(newFile);
            spiFILE.renameTo(newSPIFile);

            _writer = null;

            try {
                MinioClient minioClient = new MinioClient(BackupOptions.minioPath(),
                        BackupOptions.accessKey(),
                        BackupOptions.secretKey());

                if(!minioClient.bucketExists(BackupOptions.bucket())) {
                    minioClient.makeBucket(BackupOptions.bucket());
                }

                // Upload an object 'island.jpg' with contents from '/home/joe/island.jpg'
                minioClient.putObject(BackupOptions.bucket(), newFile.getPath(), newFile.getAbsolutePath());
                minioClient.putObject(BackupOptions.bucket(), newSPIFile.getPath(), newSPIFile.getAbsolutePath());

            } catch (Exception e){
                System.err.println("Couldn't upload file to server");
                System.out.println(e);
            }

            _isConnected = false;
            if (callback != null) {
                callback.on(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (callback != null) {
                callback.on(false);
            }
        }
    }
}
