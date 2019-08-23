package com.zeroturnaround.jc.share;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.cloudbees.jenkins.plugins.awscredentials.AmazonWebServicesCredentials;
import com.cloudbees.jenkins.plugins.sshcredentials.SSHAuthenticator;
import com.cloudbees.plugins.credentials.CredentialsProvider;
import com.cloudbees.plugins.credentials.common.StandardUsernameCredentials;
import com.trilead.ssh2.Connection;
import com.trilead.ssh2.SCPClient;
import com.trilead.ssh2.ServerHostKeyVerifier;

import hudson.FilePath;
import hudson.Launcher;
import hudson.model.AbstractBuild;
import hudson.model.BuildListener;
import hudson.model.Run;
import jenkins.model.ArtifactManager;
import jenkins.util.VirtualFile;

public class Manager extends ArtifactManager {

  private static final Logger LOG = Logger.getLogger(Manager.class.getName());

  private static final String AWS_CRED_ID = "jenkins.zing";
  private static final String SSH_CRED_ID = "ssh-zeroturnaround";
  private static final String REGION = "us-east-1";
  private static final String BUCKET = "share.jc.zt";

  private transient AbstractBuild<?, ?> build;
  private transient ServerHostKeyVerifier verifier;

  public Manager() {
  }

  public Manager(Run<?, ?> build) {
    onLoad(build);
  }

  @Override
  public void onLoad(Run<?, ?> build) {
    this.build = (AbstractBuild<?, ?>) build;
    this.verifier = new KnownHostVerifier();
  }

  @Override
  public void archive(FilePath workspace, Launcher launcher, BuildListener listener, Map<String, String> artifacts) throws IOException, InterruptedException {
    Path temp = Files.createTempDirectory("jenkinsJcShare");
    try {
      listener.getLogger().println("publishing artifacts for " + build);
      listener.getLogger().println("downloading from agent: " + artifacts);
      workspace.copyRecursiveTo(new FilePath.ExplicitlySpecifiedDirScanner(artifacts), new FilePath(temp.toFile()), getClass().getName());
      List<S3ObjectSummary> objectSummaries = copyToS3(listener, temp, artifacts);
      copyToShare("raven.jc.zt", listener, temp, artifacts);
      writeCache(objectSummaries);
      listener.getLogger().println("artifacts published");
    }
    finally {
      FileUtils.forceDelete(temp.toFile());
    }
  }

  private void writeCache(List<S3ObjectSummary> objectSummaries) throws IOException {
    Path cachePath = getObjectCachePath();
    Files.createDirectories(cachePath.getParent());
    try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(cachePath))) {
      out.writeObject(objectSummaries);
    }
  }

  @Override
  public boolean delete() throws IOException, InterruptedException {
    Files.deleteIfExists(getObjectCachePath());
    IOException ex = null;
    try {
      deleteFromShare("raven.jc.zt");
    } catch (IOException e) {
      ex = e;
    }
    deleteFromS3();

    if (ex != null)
      throw ex;
    return true;
  }

  private void deleteFromShare(String host) throws IOException, InterruptedException {
    Path basePath = getShareBase();
    Connection c = new Connection(host);
    try {
      prepareSsh(c, null);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      if (c.exec("rm -rf " + basePath, out) != 0)
        throw new IOException("host: rm -rf " + basePath + ": " + out.toString("UTF-8"));
    }
    finally {
      c.close();
    }
  }

  private void deleteFromS3() {
    Path basePath = getS3Base();
    AmazonS3 s3 = buildClient();
    try {
      List<S3ObjectSummary> objects = listAllObjects(s3, basePath);
      if (objects.isEmpty())
        return;
      s3.deleteObjects(new DeleteObjectsRequest(BUCKET).withKeys(keys(objects)));
    }
    finally {
      s3.shutdown();
    }
  }

  private String[] keys(List<S3ObjectSummary> objectSummaries) {
    return objectSummaries.stream().map(S3ObjectSummary::getKey).toArray(String[]::new);
  }

  @Override
  public VirtualFile root() {
    Path basePath = getS3Base();
    List<S3ObjectSummary> allItems = readCached();
    if (allItems == null) {
      AmazonS3 s3 = buildClient();
      try {
        allItems = listAllObjects(s3, basePath);
      }
      finally {
        s3.shutdown();
      }
    }

    SortedMap<Path, S3ObjectSummary> itemMap = new TreeMap<>();
    for (S3ObjectSummary item : allItems) {
      itemMap.put(Paths.get(item.getKey()), item);
    }

    return new JCVirtualFile(itemMap, "http://share.jc.zt/", basePath);
  }

  private List<S3ObjectSummary> readCached() {
    Path objectsCache = getObjectCachePath();
    if (!Files.isRegularFile(objectsCache)) {
      return null;
    }

    try (ObjectInputStream is = new ObjectInputStream(Files.newInputStream(objectsCache))) {
      return (List<S3ObjectSummary>) is.readObject();
    }
    catch (Exception e) {
      LOG.log(Level.SEVERE, "failed to read artifact cache of " + build, e);
      return null;
    }
  }

  private Path getObjectCachePath() {
    return build.getArtifactsDir().toPath().resolve("objects.bin");
  }

  private List<S3ObjectSummary> listAllObjects(AmazonS3 s3, Path basePath) {
    List<S3ObjectSummary> allItems = new ArrayList<>();
    String marker = null;
    do {
      ObjectListing listing = s3.listObjects(new ListObjectsRequest(BUCKET, basePath.toString(), marker, null, Integer.MAX_VALUE));
      allItems.addAll(listing.getObjectSummaries());
      marker = listing.getNextMarker();
    } while (marker != null);
    return allItems;
  }

  private void copyToShare(String host, BuildListener listener, Path temp, Map<String, String> artifacts) throws InterruptedException, IOException {
    Path basePath = getShareBase();

    listener.getLogger().println("uploading to " + host);
    Connection c = new Connection(host);
    try {
      prepareSsh(c, listener);
      SCPClient scpClient = c.createSCPClient();
      for (Map.Entry<String, String> entry : artifacts.entrySet()) {
        Path from = temp.resolve(entry.getValue());
        Path to = basePath.resolve(entry.getKey());
        listener.getLogger().println("scp: " + from + " -> " + to);
        if (c.exec("mkdir -p " + to.getParent(), listener.getLogger()) != 0)
          throw new IOException(host + ": mkdir failed");
        scpClient.put(from.toString(), to.getFileName().toString(), to.getParent().toString(), "0644");
      }
    }
    finally {
      c.close();
    }
  }

  private void prepareSsh(Connection c, BuildListener listener) throws IOException, InterruptedException {
    c.setServerHostKeyAlgorithms(new String[]{"ecdsa-sha2-nistp256"});
    c.connect(verifier);
    if (!SSHAuthenticator.newInstance(c, findSshCredential(SSH_CRED_ID)).authenticate(listener))
      throw new IOException("authenticate failed for host=" + c.getHostname() + " cred=" + SSH_CRED_ID);
  }

  private List<S3ObjectSummary> copyToS3(BuildListener listener, Path temp, Map<String, String> artifacts) throws InterruptedException {
    listener.getLogger().println("uploading to s3");
    Path basePath = getS3Base();
    TransferManager manager = TransferManagerBuilder.standard().withS3Client(buildClient()).build();
    try {
      for (Map.Entry<String, String> e : artifacts.entrySet()) {
        File localPath = temp.resolve(e.getValue()).toFile();
        String remotePath = basePath.resolve(e.getKey()).toString();
        listener.getLogger().println("s3: " + localPath + " -> " + remotePath);
        manager.upload(new PutObjectRequest(BUCKET, remotePath, localPath)).waitForCompletion();
      }
      return listAllObjects(manager.getAmazonS3Client(), basePath);
    }
    finally {
      manager.shutdownNow(true);
    }
  }

  private AmazonS3 buildClient() {
    return AmazonS3ClientBuilder.standard()
        .withCredentials(findAwsCredential(AWS_CRED_ID))
        .withRegion(REGION)
        .build();
  }

  private AmazonWebServicesCredentials findAwsCredential(String id) {
    AmazonWebServicesCredentials credential = CredentialsProvider.findCredentialById(id, AmazonWebServicesCredentials.class, build);
    if (credential == null)
      throw new NoSuchElementException(id);
    return credential;
  }

  private StandardUsernameCredentials findSshCredential(String id) {
    StandardUsernameCredentials credential = CredentialsProvider.findCredentialById(id, StandardUsernameCredentials.class, build);
    if (credential == null)
      throw new NoSuchElementException(id);
    return credential;
  }

  private Path getShareBase() {
    return Paths.get("/srv/samba/share/jenkins-artifacts", build.getProject().getName(), Integer.toString(build.getNumber()));
  }

  private Path getS3Base() {
    return Paths.get("jenkins-artifacts", build.getProject().getName(), Integer.toString(build.getNumber()));
  }
}
