/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jdvn.setl.geos.processors.shapefile;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.avro.WriteAvroResultWithSchema;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.flowfile.attributes.GeoAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StopWatch;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.filter.identity.FeatureId;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.jdvn.setl.geos.processors.util.GeoUtils;



@Tags({ "Shape file", "wkt", "json", "Features", "Attributes", "Geospatial" })
@CapabilityDescription("Read data from a given shape file and represent geospatial data in WKT format.")
@SeeAlso({ ShpWriter.class })
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })

public class ShpReader extends AbstractProcessor {
    public static final String GEO_COLUMN = "geo.column";
    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();
    
	public static final PropertyDescriptor B_FROM_DIRECTORY = new PropertyDescriptor.Builder()
			.name("Data From Directory")
			.description("If true, then use the given directory to detect data. If not, use source flowfile to process.")
			.required(true)
			.allowableValues("true", "false")
			.defaultValue("true")
			.build();	
    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Input Directory")
            .description("The input directory from which to pull files")
            .required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .dependsOn(B_FROM_DIRECTORY, "true")
            .build();
    public static final PropertyDescriptor RECURSE = new PropertyDescriptor.Builder()
            .name("Recurse Subdirectories")
            .description("Indicates whether or not to pull files from subdirectories")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .dependsOn(B_FROM_DIRECTORY, "true")
            .build();
    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
            .name("File Filter")
            .description("Only files whose names match the given regular expression will be picked up")
            .required(true)
            .defaultValue(".*\\.shp")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .dependsOn(B_FROM_DIRECTORY, "true")
            .build();
    public static final PropertyDescriptor PATH_FILTER = new PropertyDescriptor.Builder()
            .name("Path Filter")
            .description("When " + RECURSE.getName() + " is true, then only subdirectories whose path matches the given regular expression will be scanned")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .dependsOn(B_FROM_DIRECTORY, "true")
            .build();    
    public static final PropertyDescriptor DELETE_EMPTY_FLOWFILE = new PropertyDescriptor.Builder()
            .name("Delete Zero-Record FlowFiles")
            .description("If true, the FlowFile with zero records is deleted.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();    
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The character set of shapfiles to fetch")
            .required(false)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();    
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of files to pull in each iteration")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .dependsOn(B_FROM_DIRECTORY, "true")
            .build();    
    public static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("shapefile-max-rows")
            .displayName("Max Rows Per Flow File")
            .description("The maximum number of result rows that will be included in a single FlowFile. This will allow you to break up very large "
                    + "shape file into multiple FlowFiles. If the value specified is zero, then all rows are returned in a single FlowFile.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();    
    static final PropertyDescriptor INCLUDE_ZERO_RECORD_FLOWFILES = new PropertyDescriptor.Builder()
            .name("include-zero-record-flowfiles")
            .displayName("Include Zero Record FlowFiles")
            .description("When running the SQL statement against an incoming FlowFile, if the result has no data, "
                + "this property specifies whether or not a FlowFile will be sent to the corresponding relationship")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();
    public static final PropertyDescriptor IGNORE_HIDDEN_FILES = new PropertyDescriptor.Builder()
            .name("Ignore Hidden Files")
            .description("Indicates whether or not hidden files should be ignored")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .dependsOn(B_FROM_DIRECTORY, "true")
            .build();
    public static final PropertyDescriptor POLLING_INTERVAL = new PropertyDescriptor.Builder()
            .name("Polling Interval")
            .description("Indicates how long to wait before performing a directory listing")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .dependsOn(B_FROM_DIRECTORY, "true")
            .build();    
    public static final String FILE_CREATION_TIME_ATTRIBUTE = "file.creationTime";
    public static final String FILE_LAST_MODIFY_TIME_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_LAST_ACCESS_TIME_ATTRIBUTE = "file.lastAccessTime";
    public static final String FILE_OWNER_ATTRIBUTE = "file.owner";
    public static final String FILE_GROUP_ATTRIBUTE = "file.group";
    public static final String FILE_PERMISSIONS_ATTRIBUTE = "file.permissions";
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All files are routed to success").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private final AtomicReference<FileFilter> fileFilterRef = new AtomicReference<>();

    private final BlockingQueue<File> fileQueue = new LinkedBlockingQueue<>();
    private final Set<File> inProcess = new HashSet<>();    // guarded by queueLock
    private final Set<File> recentlyProcessed = new HashSet<>();    // guarded by queueLock
    private final Lock queueLock = new ReentrantLock();

    private final Lock listingLock = new ReentrantLock();

    private final AtomicLong queueLastUpdated = new AtomicLong(0L);

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(B_FROM_DIRECTORY);
        properties.add(DIRECTORY);
        properties.add(FILE_FILTER);
        properties.add(PATH_FILTER);
        properties.add(BATCH_SIZE);
        properties.add(MAX_ROWS_PER_FLOW_FILE);
        properties.add(DELETE_EMPTY_FLOWFILE);  
        properties.add(CHARSET);        
        properties.add(RECURSE);
        properties.add(POLLING_INTERVAL);
        properties.add(IGNORE_HIDDEN_FILES);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        fileFilterRef.set(createFileFilter(context));
        fileQueue.clear();
    }

	private void writeShpFileToAvroRecordSet(final ProcessSession session, final File Shpfile, final String dirLocation, final String charset,
			final Integer maxRowsPerFlowFile, boolean deleteZeroFlowFile, final ComponentLog logger) {
		
		String geoName  = Shpfile.getName().substring(0, Shpfile.getName().lastIndexOf('.'));
		String geo_url  = dirLocation == null ? Shpfile.toURI().toString() : dirLocation + "/" + Shpfile.getName();
//		String path     = dirLocation == null ? Shpfile.getPath() : dirLocation + "/" + Shpfile.getName();
//		String abs_path = dirLocation == null ? Shpfile.getAbsolutePath() : dirLocation + "/" + Shpfile.getName();
		
		final Path filePath = Shpfile.toPath();
		FlowFile flowFile = session.create();                
        flowFile = session.importFrom(filePath, true, flowFile);
        flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), Shpfile.getName());
        flowFile = session.putAttribute(flowFile, CoreAttributes.PATH.key(), Shpfile.getPath());
        flowFile = session.putAttribute(flowFile, CoreAttributes.ABSOLUTE_PATH.key(), Shpfile.getAbsolutePath());
        Map<String, String> attributes = getAttributesFromFile(filePath);
        if (attributes.size() > 0) {
            flowFile = session.putAllAttributes(flowFile, attributes);
        }
		
		Map<String, Object> mapAttrs = new HashMap<>();
		try {
			mapAttrs.put("url", Shpfile.toURI().toURL());
			DataStore dataStore = DataStoreFinder.getDataStore(mapAttrs);
			String typeName = dataStore.getTypeNames()[0];
			SimpleFeatureSource featureSource = dataStore.getFeatureSource(typeName);
			// Center and envelope for all features, for fragments in to re-calculate
			String center = null;
			String envelope = null;
			int maxRecord = featureSource.getFeatures().size(); // Call ones like this before getCharset function, why?
			if (maxRecord > 0) {
				ReferencedEnvelope r = featureSource.getBounds();
				center = "[" + String.valueOf(r.centre().getX()) + "," + String.valueOf(r.centre().getY()) + "]";
				envelope = "[[" + String.valueOf(r.getMinX()) + "," + String.valueOf(r.getMaxX()) + "]" + ", ["
						+ String.valueOf(r.getMinY()) + "," + String.valueOf(r.getMaxY()) + "]]";
			}
			if (charset != null)
				((ShapefileDataStore) dataStore).setCharset(Charset.forName(charset));
			Charset charset_in = ((ShapefileDataStore) dataStore).getCharset();
			if (maxRowsPerFlowFile > 0 && maxRowsPerFlowFile < maxRecord) {
				int from = 0;
				int to = 0;
				final String fragmentIdentifier = UUID.randomUUID().toString();
				int fragmentIndex = 0;
				final RecordSchema recordSchema = GeoUtils.createFeatureRecordSchema(featureSource);
				List<FeatureId> featureIds = GeoUtils.getFeatureIds(featureSource.getFeatures());

				while (from < maxRecord) {
					final StopWatch stopWatch = new StopWatch(true);
					to = from + maxRowsPerFlowFile;
					if (to > maxRecord)
						to = maxRecord;
					Set<FeatureId> selectedIds = new LinkedHashSet<FeatureId>(featureIds.subList(from, to));
					List<Record> records = GeoUtils.getNifiRecordSegmentsFromFeatureSource(featureSource, recordSchema,
							selectedIds, charset_in);
					if (records.size() > 0) {
						FlowFile transformed = session.create(flowFile);
						CoordinateReferenceSystem myCrs = featureSource.getSchema().getCoordinateReferenceSystem();
						transformed = session.write(transformed, new OutputStreamCallback() {
							@Override
							public void process(final OutputStream out) throws IOException {
								final Schema avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
								@SuppressWarnings("resource")
								final RecordSetWriter writer = new WriteAvroResultWithSchema(avroSchema, out,
										CodecFactory.bzip2Codec());
								writer.write(new ListRecordSet(recordSchema, records));
								writer.flush();
							}
						});

						transformed = session.putAttribute(transformed, GeoAttributes.GEO_TYPE.key(), "Features");
						transformed = session.putAttribute(transformed, GeoUtils.GEO_DB_SRC_TYPE, "Shape file");
						transformed = session.putAttribute(transformed, GeoAttributes.GEO_NAME.key(),
								geoName + ":" + fragmentIdentifier + ":" + String.valueOf(fragmentIndex));
						transformed = session.putAttribute(transformed, GEO_COLUMN, GeoUtils.SHP_GEO_COLUMN);
						transformed = session.putAttribute(transformed, GeoAttributes.GEO_CENTER.key(), center);
						transformed = session.putAttribute(transformed, GeoAttributes.GEO_ENVELOPE.key(), envelope);
						transformed = session.putAttribute(transformed, GeoUtils.GEO_URL, geo_url);
						transformed = session.putAttribute(transformed, GeoUtils.GEO_CHAR_SET, charset_in.name());
						transformed = session.putAttribute(transformed, GeoAttributes.GEO_RECORD_NUM.key(),
								String.valueOf(records.size()));
						transformed = session.putAttribute(transformed, FRAGMENT_ID, fragmentIdentifier);
						transformed = session.putAttribute(transformed, FRAGMENT_INDEX, String.valueOf(fragmentIndex));
						if (myCrs != null) {
							transformed = session.putAttribute(transformed, GeoAttributes.CRS.key(), myCrs.toWKT());
							transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(),
									"application/avro+geowkt");
						}

						session.getProvenanceReporter().receive(transformed, Shpfile.toURI().toString(),
								stopWatch.getElapsed(TimeUnit.MILLISECONDS));
						logger.info("added {} to flow", new Object[] { transformed });
						session.adjustCounter("Records Read", records.size(), false);
						session.transfer(transformed, REL_SUCCESS);
					}
					from = to;
					fragmentIndex++;

				}
				dataStore.dispose();
				session.remove(flowFile);

			} else {
				final StopWatch stopWatch = new StopWatch(true);
				final List<Record> records = GeoUtils.getNifiRecordsFromFeatureSource(featureSource, charset_in);
				FlowFile transformed = session.create(flowFile);
				CoordinateReferenceSystem myCrs = featureSource.getSchema().getCoordinateReferenceSystem();
				if (records.size() > 0) {
					RecordSchema recordSchema = records.get(0).getSchema();
					transformed = session.write(transformed, new OutputStreamCallback() {
						@Override
						public void process(final OutputStream out) throws IOException {
							final Schema avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
							@SuppressWarnings("resource")
							final RecordSetWriter writer = new WriteAvroResultWithSchema(avroSchema, out,
									CodecFactory.bzip2Codec());
							writer.write(new ListRecordSet(recordSchema, records));
							writer.flush();
						}
					});
				}
				if (records.size() == 0 && deleteZeroFlowFile == true) {
					session.remove(flowFile);
					logger.info("deleted {} from flow", new Object[] { transformed });
					dataStore.dispose();
					session.remove(transformed);
				} else {
					session.remove(flowFile);
					transformed = session.putAttribute(transformed, GeoAttributes.GEO_TYPE.key(), "Features");
					transformed = session.putAttribute(transformed, GeoUtils.GEO_DB_SRC_TYPE, "Shape file");
					transformed = session.putAttribute(transformed, GeoAttributes.GEO_NAME.key(), geoName);
					transformed = session.putAttribute(transformed, GEO_COLUMN, GeoUtils.SHP_GEO_COLUMN);
					transformed = session.putAttribute(transformed, GeoAttributes.GEO_CENTER.key(), center);
					transformed = session.putAttribute(transformed, GeoAttributes.GEO_ENVELOPE.key(), envelope);
					transformed = session.putAttribute(transformed, GeoUtils.GEO_URL, geo_url);
					transformed = session.putAttribute(transformed, GeoUtils.GEO_CHAR_SET, charset_in.name());
					transformed = session.putAttribute(transformed, GeoAttributes.GEO_RECORD_NUM.key(),
							String.valueOf(records.size()));
					if (myCrs != null) {
						transformed = session.putAttribute(transformed, GeoAttributes.CRS.key(), myCrs.toWKT());
						transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(),
								"application/avro+geowkt");
					}
					session.getProvenanceReporter().receive(transformed, Shpfile.toURI().toString(),
							stopWatch.getElapsed(TimeUnit.MILLISECONDS));
					logger.info("added {} to flow", new Object[] { transformed });
					dataStore.dispose();
					session.adjustCounter("Records Read", records.size(), false);
					session.transfer(transformed, REL_SUCCESS);
				}

			}			
		} catch (IOException e) {
			logger.error("Failed to retrieve file {} due to {}",  new Object[] { Shpfile, e });
            if (flowFile != null) {
                session.remove(flowFile);
            }
		}
	}	
	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) {

		final boolean b_fromflow = context.getProperty(B_FROM_DIRECTORY).asBoolean();        
        final boolean deleteZeroFlowFile = context.getProperty(DELETE_EMPTY_FLOWFILE).asBoolean();
        final String charset = context.getProperty(CHARSET).evaluateAttributeExpressions().getValue();
        
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
        final ComponentLog logger = getLogger();
        if (b_fromflow) {
        	final File directory = new File(context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue());
            if (fileQueue.size() < 100) {
                final long pollingMillis = context.getProperty(POLLING_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
                if ((queueLastUpdated.get() < System.currentTimeMillis() - pollingMillis) && listingLock.tryLock()) {
                    try {
                        final Set<File> listing = performListing(directory, fileFilterRef.get(), context.getProperty(RECURSE).asBoolean().booleanValue());

                        queueLock.lock();
                        try {
                            listing.removeAll(inProcess);
                            fileQueue.clear();
                            fileQueue.addAll(listing);

                            queueLastUpdated.set(System.currentTimeMillis());
                            recentlyProcessed.clear();

                            if (listing.isEmpty()) {
                                context.yield();
                            }
                        } finally {
                            queueLock.unlock();
                        }
                    } finally {
                        listingLock.unlock();
                    }
                }
            }

            final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
            final List<File> files = new ArrayList<>(batchSize);
            queueLock.lock();
            try {
                fileQueue.drainTo(files, batchSize);
                if (files.isEmpty()) {
                    return;
                } else {
                    inProcess.addAll(files);
                }
            } finally {
                queueLock.unlock();
            }

            final ListIterator<File> itr = files.listIterator();
            File currentfile = null;
            try {
                while (itr.hasNext()) {
                    final File file = itr.next();
                    currentfile = file;
                    writeShpFileToAvroRecordSet(session, file, null, charset, maxRowsPerFlowFile, deleteZeroFlowFile, logger);                    
                    if (!isScheduled()) {  // if processor stopped, put the rest of the files back on the queue.
                        queueLock.lock();
                        try {
                            while (itr.hasNext()) {
                                final File nextFile = itr.next();
                                fileQueue.add(nextFile);
                                inProcess.remove(nextFile);
                            }
                        } finally {
                            queueLock.unlock();
                        }
                    }
                }
            } catch (final Exception e) {
                logger.error("Failed to retrieve file {} due to {}",  new Object[] { currentfile, e });
            } finally {
                queueLock.lock();
                try {
                    inProcess.removeAll(files);
                    recentlyProcessed.addAll(files);
                } finally {
                    queueLock.unlock();
                }
            }        	
        }
        else {        	
			FlowFile flowFile = session.get();
			if (flowFile == null) {
				return;
			}
			try {
				session.read(flowFile, new InputStreamCallback() {
					@Override
					public void process(final InputStream in) {						
						String targetDir = flowFile.getAttributes().get("filename");
						targetDir = targetDir.substring(0, targetDir.length() - 4); // remove .zip extension
						
						String geoName = flowFile.getAttributes().get(GeoUtils.SHP_ZIP_MARK);
						String dirLocation = flowFile.getAttributes().get(GeoUtils.SHP_ZIP_LOCATION);
						
					    File TEMP_UNZIP_DIR = new File(System.getProperty("java.io.tmpdir") + File.separator + targetDir);
						try {
						    System.out.println(System.getProperty("java.io.tmpdir"));
						    System.out.println("Unzip at : " + TEMP_UNZIP_DIR.getPath());
						    
						    Path destPath = TEMP_UNZIP_DIR.toPath();						    
					        ZipInputStream zis = new ZipInputStream(in);
					        ZipEntry zipEntry;
					        // while there are entries I process them
					        while ((zipEntry = zis.getNextEntry()) != null)
					        {
								Path resolvedPath = destPath.resolve(zipEntry.getName().substring(zipEntry.getName().lastIndexOf("/")+1)).normalize();
								if (!resolvedPath.startsWith(destPath)) {
									throw new IOException("The requested zip-entry '" + zipEntry.getName()
											+ "' does not belong to the requested destination");
								}
								if (zipEntry.isDirectory()) {
									Files.createDirectories(resolvedPath);
								} 
								else {
									if (!Files.isDirectory(resolvedPath.getParent())) {
										Files.createDirectories(resolvedPath.getParent());
									}
									try (FileOutputStream outStream = new FileOutputStream(resolvedPath.toFile())) {
										IOUtils.copy(zis, outStream);
									}
								}
					        }
					        final File shpFile = new File(TEMP_UNZIP_DIR.toPath() + "/" + geoName + ".shp");
					        writeShpFileToAvroRecordSet(session, shpFile, dirLocation, charset, maxRowsPerFlowFile, deleteZeroFlowFile, logger);
					        // Clean files in temp folder
					        FileUtils.deleteDirectory(TEMP_UNZIP_DIR);
						} catch (IOException e) {
							logger.error("Could not transformed {} because {}", new Object[] { flowFile, e });
						}
						finally {
							
						}
					}
				});
				session.remove(flowFile);

			} catch (Exception e) {
				logger.error("Could not transformed {} because {}", new Object[] { flowFile, e });
			}
        }
    }        

	
    private FileFilter createFileFilter(final ProcessContext context) {
        final boolean ignoreHidden = context.getProperty(IGNORE_HIDDEN_FILES).asBoolean();
        final Pattern filePattern = Pattern.compile(context.getProperty(FILE_FILTER).getValue());
        final String indir = context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
        final boolean recurseDirs = context.getProperty(RECURSE).asBoolean();
        final String pathPatternStr = context.getProperty(PATH_FILTER).getValue();
        final Pattern pathPattern = (!recurseDirs || pathPatternStr == null) ? null : Pattern.compile(pathPatternStr);

        return new FileFilter() {
            @Override
            public boolean accept(final File file) {
                if (ignoreHidden && file.isHidden()) {
                    return false;
                }
                if (pathPattern != null) {
                    Path reldir = Paths.get(indir).relativize(file.toPath()).getParent();
                    if (reldir != null && !reldir.toString().isEmpty()) {
                        if (!pathPattern.matcher(reldir.toString()).matches()) {
                            return false;
                        }
                    }
                }
                //Verify that we have at least read permissions on the file we're considering grabbing
                if (!Files.isReadable(file.toPath())) {
                    return false;
                }
                return filePattern.matcher(file.getName()).matches();
            }
        };
    }

    private Set<File> performListing(final File directory, final FileFilter filter, final boolean recurseSubdirectories) {
        Path p = directory.toPath();
        if (!Files.isWritable(p) || !Files.isReadable(p)) {
            throw new IllegalStateException("Directory '" + directory + "' does not have sufficient permissions (i.e., not writable and readable)");
        }
        final Set<File> queue = new HashSet<>();
        if (!directory.exists()) {
            return queue;
        }

        final File[] children = directory.listFiles();
        if (children == null) {
            return queue;
        }

        for (final File child : children) {
            if (child.isDirectory()) {
                if (recurseSubdirectories) {
                    queue.addAll(performListing(child, filter, recurseSubdirectories));
                }
            } else if (filter.accept(child)) {
                queue.add(child);
            }
        }

        return queue;
    }	
    protected Map<String, String> getAttributesFromFile(final Path file) {
        Map<String, String> attributes = new HashMap<>();
        try {
            FileStore store = Files.getFileStore(file);
            if (store.supportsFileAttributeView("basic")) {
                try {
                    final DateFormat formatter = new SimpleDateFormat(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
                    BasicFileAttributeView view = Files.getFileAttributeView(file, BasicFileAttributeView.class);
                    BasicFileAttributes attrs = view.readAttributes();
                    attributes.put(FILE_LAST_MODIFY_TIME_ATTRIBUTE, formatter.format(new Date(attrs.lastModifiedTime().toMillis())));
                    attributes.put(FILE_CREATION_TIME_ATTRIBUTE, formatter.format(new Date(attrs.creationTime().toMillis())));
                    attributes.put(FILE_LAST_ACCESS_TIME_ATTRIBUTE, formatter.format(new Date(attrs.lastAccessTime().toMillis())));
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
            if (store.supportsFileAttributeView("owner")) {
                try {
                    FileOwnerAttributeView view = Files.getFileAttributeView(file, FileOwnerAttributeView.class);
                    attributes.put(FILE_OWNER_ATTRIBUTE, view.getOwner().getName());
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
            if (store.supportsFileAttributeView("posix")) {
                try {
                    PosixFileAttributeView view = Files.getFileAttributeView(file, PosixFileAttributeView.class);
                    attributes.put(FILE_PERMISSIONS_ATTRIBUTE, PosixFilePermissions.toString(view.readAttributes().permissions()));
                    attributes.put(FILE_GROUP_ATTRIBUTE, view.readAttributes().group().getName());
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
        } catch (IOException ioe) {
            // well then this FlowFile gets none of these attributes
        }

        return attributes;
    }    

}
