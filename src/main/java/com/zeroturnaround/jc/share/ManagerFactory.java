package com.zeroturnaround.jc.share;

import javax.annotation.CheckForNull;

import org.kohsuke.stapler.DataBoundConstructor;

import hudson.Extension;
import hudson.model.AbstractBuild;
import hudson.model.Run;
import jenkins.model.ArtifactManager;
import jenkins.model.ArtifactManagerFactory;
import jenkins.model.ArtifactManagerFactoryDescriptor;

public class ManagerFactory extends ArtifactManagerFactory {

  @DataBoundConstructor
  public ManagerFactory() {
  }

  @CheckForNull
  @Override
  public ArtifactManager managerFor(Run<?, ?> build) {
    if (build instanceof AbstractBuild) {
      AbstractBuild b = (AbstractBuild) build;
      if ("jr-agent-copy-artifacts".equals(b.getProject().getName())) {
        return new Manager(build);
      }
    }
    return null;
  }

  @Extension
  public static final class DescriptorImpl extends ArtifactManagerFactoryDescriptor {
    @Override
    public String getDisplayName() {
      return "Mirror artifacts from jr-agent-copy-artifacts into zt-devel S3, raven.jc.zt and fatboy.jc.zt";
    }
  }
}
