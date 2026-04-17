#!groovy

import hudson.security.FullControlOnceLoggedInAuthorizationStrategy
import jenkins.model.Jenkins
import hudson.security.HudsonPrivateSecurityRealm

def instance = Jenkins.get()

def hudsonRealm = new HudsonPrivateSecurityRealm(false)
hudsonRealm.createAccount("admin", "admin")
instance.setSecurityRealm(hudsonRealm)

def strategy = new FullControlOnceLoggedInAuthorizationStrategy()
strategy.setAllowAnonymousRead(false)
instance.setAuthorizationStrategy(strategy)

instance.save()
