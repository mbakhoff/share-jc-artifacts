package com.zeroturnaround.jc.share;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.util.SortedMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import jenkins.util.VirtualFile;

public class JCVirtualFile extends VirtualFile {

  private final SortedMap<Path, S3ObjectSummary> objects;
  private final String urlBase;
  private final Path path;

  public JCVirtualFile(SortedMap<Path, S3ObjectSummary> objects, String urlBase, Path path) {
    this.objects = objects;
    this.urlBase = urlBase;
    this.path = path;
  }

  @Nonnull
  @Override
  public String getName() {
    return path.getFileName().toString();
  }

  @Nonnull
  @Override
  public URI toURI() {
    return URI.create(urlBase + path);
  }

  @Override
  public VirtualFile getParent() {
    Path parent = path.getParent();
    return parent == null ? null : new JCVirtualFile(objects, urlBase, parent);
  }

  @Override
  public boolean isDirectory() {
    if (isFile())
      return false;
    SortedMap<Path, S3ObjectSummary> tail = objects.tailMap(path);
    return !tail.isEmpty() && tail.firstKey().startsWith(path);
  }

  @Override
  public boolean isFile() {
    return objects.get(path) != null;
  }

  @Override
  public boolean exists() {
    return isFile() || isDirectory();
  }

  @Nonnull
  @Override
  public VirtualFile[] list() {
    if (!isDirectory())
      return new VirtualFile[0];
    return objects.keySet().stream()
        .filter(p -> p.startsWith(path))
        .map(p -> nextFromPrefix(p, path))
        .distinct()
        .map(p -> new JCVirtualFile(objects, urlBase, p))
        .toArray(VirtualFile[]::new);
  }

  private Path nextFromPrefix(Path key, Path pathPrefix) {
    return pathPrefix.resolve(key.getName(pathPrefix.getNameCount()));
  }

  @Nonnull
  @Override
  public VirtualFile child(@Nonnull String name) {
    return new JCVirtualFile(objects, urlBase, path.resolve(name));
  }

  @Override
  public long length() {
    S3ObjectSummary summary = objects.get(path);
    return summary != null ? summary.getSize() : 0;
  }

  @Override
  public long lastModified() {
    S3ObjectSummary summary = objects.get(path);
    return summary != null ? summary.getLastModified().getTime() : 0;
  }

  @Override
  public boolean canRead() {
    return exists();
  }

  @Override
  public InputStream open() throws IOException {
    if (!isFile())
      throw new IOException("not a file: " + path);
    return toURI().toURL().openStream();
  }

  @CheckForNull
  @Override
  public URL toExternalURL() throws IOException {
    if (!isFile())
      return null;
    return toURI().toURL();
  }
}
